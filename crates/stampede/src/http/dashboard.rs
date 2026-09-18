//! The operator console, served with Datastar over SSE.
//!
//! A job dashboard is ~90% "stream state changes into a table", which is
//! exactly SSE's shape (plans/06). Sidekiq Web is ERB plus polling; GoodJob is
//! Rails plus Turbo; River UI is a React SPA with its own build pipeline. This
//! is one HTML page and a server-sent event stream — no Node toolchain, no
//! bundler, nothing to `npm install`. Datastar patches elements by selector, so
//! all rendering stays in Rust.
//!
//! **Off unless a token is configured.** plans/06 asks for secure-by-default
//! and names the precedent: an opt-in auth setting meant a job dashboard sat
//! open on staging and production because nobody opted in. This one exposes job
//! args, which are customer data.
//!
//! Every query here obeys the plans/13 rule: no unbounded aggregate over the
//! job table. Depth comes from the counter table, failures cluster on the
//! fingerprint index, and the job list is `LIMIT`ed. This page refreshes every
//! second for as long as someone leaves it open, which makes it the single most
//! dangerous place in the system to run a `count(*)`.

use super::ApiState;
use rama::futures::stream;
use rama::http::StatusCode;
use rama::http::header;
use rama::http::service::web::extract::{Query, State};
use rama::http::service::web::response::{Html, IntoResponse, Sse};
use rama::http::sse::Event;
use rama::http::sse::datastar::{ElementPatchMode, PatchElements};
use rama::http::sse::server::KeepAlive;
use serde::Deserialize;
use std::convert::Infallible;
use std::time::Duration;

/// How far back the destination and failure panels look.
const WINDOW: Duration = Duration::from_secs(3600);

/// Rows in the job list. Small on purpose — see `Queue::recent_jobs` for why
/// this is not free on a large partition.
const JOB_LIMIT: i64 = 40;

const INDEX: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Stampede</title>
<script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js"></script>
<style>
  :root { color-scheme: dark light; --dim: color-mix(in srgb, currentColor 55%, transparent) }
  body { font: 13px/1.55 ui-monospace, SFMono-Regular, Menlo, monospace; margin: 1.5rem 2rem; max-width: 100rem }
  header { display: flex; align-items: baseline; gap: 1.5rem; margin-bottom: 1rem }
  h1 { font-size: 1rem; letter-spacing: .18em; text-transform: uppercase; margin: 0 }
  h2 { font-size: .72rem; letter-spacing: .12em; text-transform: uppercase;
       color: var(--dim); margin: 1.6rem 0 .35rem; font-weight: 600 }
  table { border-collapse: collapse; width: 100% }
  th { text-align: left; font-weight: 600; color: var(--dim); border-bottom: 1px solid var(--dim);
       padding: .2rem .9rem .2rem 0; font-size: .7rem; letter-spacing: .06em; text-transform: uppercase }
  td { padding: .22rem .9rem .22rem 0; border-bottom: 1px solid color-mix(in srgb, currentColor 8%, transparent);
       white-space: nowrap; overflow: hidden; text-overflow: ellipsis }
  .n { text-align: right; font-variant-numeric: tabular-nums }
  .bad { color: #d33 } .warn { color: #c80 } .ok { color: #2a2 }
  .dim { color: var(--dim) }
  .wide { white-space: normal; max-width: 38rem }
  nav { margin: .5rem 0 }
  nav a { cursor: pointer; margin-right: .9rem; text-decoration: none; border-bottom: 1px dotted }
  nav a.on { font-weight: 700; border-bottom: 2px solid }
  footer { color: var(--dim); margin-top: 2.5rem; font-size: .72rem }
</style>
</head>
<body data-on-load="@get('/dashboard/stream?token=__TOKEN__&amp;q=__Q__&amp;s=__S__&amp;j=__J__')">
  <header><h1>Stampede</h1><span id="wakeups"></span></header>
  <div id="queues">connecting…</div>
  <div id="destinations"></div>
  <div id="failures"></div>
  <div id="job"></div>
  <div id="jobs"></div>
  <div id="schedules"></div>
  <div id="health"></div>
  <footer>live over server-sent events · read-only</footer>
</body>
</html>"##;

/// Dashboard credentials and filters.
///
/// The token rides on the query string because the page must hand it to the SSE
/// stream and `EventSource` cannot set headers. That puts the secret in server
/// logs and browser history — the honest cost of a dependency-free auth model.
/// Deployments that care should front this with their own proxy and set no
/// token at all.
#[derive(Debug, Deserialize, Default)]
pub struct Params {
    #[serde(default)]
    pub token: Option<String>,
    /// Queue filter.
    #[serde(default)]
    pub q: Option<String>,
    /// State filter.
    #[serde(default)]
    pub s: Option<String>,
    /// A single job to expand, by id.
    #[serde(default)]
    pub j: Option<String>,
}

impl Params {
    fn queue(&self) -> &str {
        self.q.as_deref().filter(|q| !q.is_empty()).unwrap_or("default")
    }

    fn state(&self) -> Option<&str> {
        self.s.as_deref().filter(|s| !s.is_empty())
    }

    fn tok(&self) -> String {
        esc(self.token.as_deref().unwrap_or(""))
    }

    fn job(&self) -> Option<&str> {
        self.j.as_deref().filter(|j| !j.is_empty())
    }

    /// A stream URL with one field swapped. `Some("")` clears a field.
    fn link(&self, q: Option<&str>, st: Option<&str>, job: Option<&str>) -> String {
        format!(
            "/dashboard/stream?token={}&amp;q={}&amp;s={}&amp;j={}",
            self.tok(),
            esc(q.unwrap_or(self.queue())),
            esc(st.unwrap_or(self.state().unwrap_or(""))),
            esc(job.unwrap_or(self.job().unwrap_or(""))),
        )
    }
}

pub async fn index(State(state): State<ApiState>, Query(p): Query<Params>) -> impl IntoResponse {
    if !state.dashboard_authorised(p.token.as_deref()) {
        // 404, not 401: an unauthenticated caller has no business learning
        // that a dashboard exists here.
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    Html(
        INDEX
            .replace("__TOKEN__", &p.tok())
            .replace("__Q__", &esc(p.queue()))
            .replace("__S__", &esc(p.state().unwrap_or("")))
            .replace("__J__", &esc(p.job().unwrap_or(""))),
    )
    .into_response()
}

/// The live stream.
///
/// Polled rather than driven by LISTEN/NOTIFY: a dashboard is allowed to be a
/// second stale, and a notification per job transition would put dashboard load
/// on the channel workers depend on for wakeups (plans/12).
pub async fn stream(State(state): State<ApiState>, Query(p): Query<Params>) -> impl IntoResponse {
    if !state.dashboard_authorised(p.token.as_deref()) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    let ticks = stream::unfold(
        (state, p, Vec::<Event<PatchElements>>::new(), true),
        move |(state, p, mut pending, first)| async move {
            if let Some(ev) = pending.pop() {
                return Some((Ok::<_, Infallible>(ev), (state, p, pending, false)));
            }

            // The first tick renders immediately; "connecting…" for a second
            // makes a working dashboard look broken.
            if !first {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // Built in reverse so `pop` emits them top-down.
            let mut next: Vec<_> = [
                render_health(&state).await,
                render_schedules(&state).await,
                render_job(&state, &p).await,
                render_jobs(&state, &p).await,
                render_failures(&state).await,
                render_destinations(&state).await,
                render_queues(&state, &p).await,
                render_wakeups(&state),
            ]
            .into_iter()
            .flatten()
            .collect();

            let head = next.pop()?;
            Some((Ok(head), (state, p, next, false)))
        },
    );

    (
        [(header::CACHE_CONTROL, "no-cache")],
        Sse::new(ticks).with_keep_alive(KeepAlive::new()),
    )
        .into_response()
}

fn render_wakeups(state: &ApiState) -> Option<Event<PatchElements>> {
    // Not an error either way -- the queue is correct when polling -- but worth
    // seeing before someone spends an afternoon wondering about latency.
    let (class, text) = if state.notifications_live {
        ("ok", "wakeups live")
    } else {
        ("warn", "polling only")
    };
    patch(format!(r#"<span id="wakeups" class="{class}">{text}</span>"#))
}

async fn render_queues(state: &ApiState, p: &Params) -> Option<Event<PatchElements>> {
    // The counter table, never count(*) over the jobs table. This runs once a
    // second for as long as the page is open, which is exactly how an unbounded
    // aggregate pins the xmin horizon and stops vacuum reclaiming (plans/13).
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT queue, state, n FROM stampede_queue_counts WHERE n <> 0 ORDER BY queue, state",
    )
    .fetch_all(state.queue.pool())
    .await
    .ok()?;

    let ages = state.queue.oldest_available_age().await.unwrap_or_default();

    let mut html = String::from(
        r#"<div id="queues"><h2>queues</h2><table>"#,
    );
    html.push_str(r#"<tr><th>queue</th><th>state</th><th class="n">jobs</th><th class="n">oldest</th></tr>"#);
    if rows.is_empty() {
        html.push_str(r#"<tr><td colspan="4" class="dim">no jobs</td></tr>"#);
    }
    for (queue, st, n) in rows {
        // Only for states that are actually waiting: the oldest COMPLETED job
        // is retention, not backlog.
        let age = if st == "available" || st == "retryable" {
            ages.iter()
                .find(|(q, _)| *q == queue)
                .map(|(_, a)| format_age(*a))
                .unwrap_or_default()
        } else {
            String::new()
        };
        let on = if queue == p.queue() { r#" class="on""# } else { "" };
        html.push_str(&format!(
            r#"<tr><td><a{on} data-on-click="@get('{href}')">{q}</a></td><td>{st}</td><td class="n">{n}</td><td class="n dim">{age}</td></tr>"#,
            href = p.link(Some(&queue), None, Some("")),
            q = esc(&queue),
            st = esc(&st),
        ));
    }
    html.push_str("</table></div>");
    patch(html)
}

/// Destination health — the surface plans/06 singles out as the differentiator.
///
/// **This is NOT a live read of any worker's circuit breaker.**
/// `breaker-machines` state lives in the worker process's memory, and the
/// dashboard is normally served by a `--no-worker` daemon: a different process,
/// usually a different machine. Nothing here can see it, and pretending
/// otherwise would be worse than showing less.
///
/// What it shows is the evidence a breaker reacts to, which is what an operator
/// actually needs. A run of `refused` means some worker's circuit IS open,
/// because that is what `breaker_open` records; a run of `failed` against one
/// host means one is about to be.
async fn render_destinations(state: &ApiState) -> Option<Event<PatchElements>> {
    let rows = state.queue.destination_health(WINDOW).await.ok()?;
    if rows.is_empty() {
        return patch(r#"<div id="destinations"></div>"#.to_string());
    }

    let mut html = String::from(
        r#"<div id="destinations"><h2>destinations <span class="dim">— observed from job rows, last hour</span></h2><table><tr><th>destination</th><th class="n">refused</th><th class="n">throttled</th><th class="n">failed</th><th class="n">waiting</th><th>state</th></tr>"#,
    );
    for r in rows {
        let (class, label) = if r.refused > 0 {
            ("bad", "circuit open")
        } else if r.failed > 0 {
            ("warn", "failing")
        } else {
            ("dim", "—")
        };
        html.push_str(&format!(
            r#"<tr><td>{d}</td><td class="n">{refused}</td><td class="n">{thr}</td><td class="n">{fail}</td><td class="n">{wait}</td><td class="{class}">{label}</td></tr>"#,
            d = esc(r.destination.as_deref().unwrap_or("—")),
            refused = r.refused,
            thr = r.throttled,
            fail = r.failed,
            wait = r.waiting,
        ));
    }
    html.push_str("</table></div>");
    patch(html)
}

async fn render_failures(state: &ApiState) -> Option<Event<PatchElements>> {
    let rows = state.queue.failure_clusters(8).await.ok()?;
    if rows.is_empty() {
        return patch(r#"<div id="failures"></div>"#.to_string());
    }

    let mut html = String::from(
        r#"<div id="failures"><h2>failures <span class="dim">— clustered by fingerprint</span></h2><table><tr><th class="n">n</th><th>kind</th><th>error</th><th>destination</th></tr>"#,
    );
    for r in rows {
        let kind = r.failure_kind.unwrap_or_default();
        // The kind IS the diagnosis, so it is coloured by WHO IS AT FAULT:
        // worker_* is infrastructure, breaker_open/throttled is us declining to
        // run it, raised/timed_out is the job itself.
        let class = match kind.as_str() {
            "worker_exited" | "worker_missing" | "heartbeat_lost" => "warn",
            "breaker_open" | "throttled" => "dim",
            _ => "bad",
        };
        html.push_str(&format!(
            r#"<tr><td class="n">{n}</td><td class="{class}">{kind}</td><td class="wide dim">{err}</td><td class="dim">{dest}</td></tr>"#,
            n = r.n,
            kind = esc(&kind),
            err = esc(r.sample.as_deref().unwrap_or("")),
            dest = esc(r.destination.as_deref().unwrap_or("")),
        ));
    }
    html.push_str("</table></div>");
    patch(html)
}

async fn render_schedules(state: &ApiState) -> Option<Event<PatchElements>> {
    let rows = state.queue.schedules().await.ok()?;
    if rows.is_empty() {
        return patch(r#"<div id="schedules"></div>"#.to_string());
    }

    let mut html = String::from(
        r#"<div id="schedules"><h2>schedules</h2><table><tr><th>key</th><th>rule</th><th>task</th><th>next</th><th>last</th></tr>"#,
    );
    for r in rows {
        let rule = match (&r.cron_expr, r.interval_secs) {
            (Some(c), _) => format!("{} {} ({})", esc(c), esc(&r.timezone), esc(&r.dst_policy)),
            (_, Some(n)) => format!("every {n}s"),
            _ => "—".into(),
        };
        html.push_str(&format!(
            r#"<tr><td class="{cls}">{key}{tag}</td><td class="dim">{rule}</td><td>{task}</td><td class="dim">{next}</td><td class="dim">{last}</td></tr>"#,
            cls = if r.paused { "warn" } else { "" },
            key = esc(&r.key),
            tag = if r.paused { " [paused]" } else { "" },
            task = esc(&r.task),
            next = r.next_run_at.format("%H:%M:%S"),
            last = r
                .last_run_at
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "never".into()),
        ));
    }
    html.push_str("</table></div>");
    patch(html)
}

async fn render_jobs(state: &ApiState, p: &Params) -> Option<Event<PatchElements>> {
    let jobs = match state.queue.recent_jobs(p.queue(), p.state(), JOB_LIMIT).await {
        Ok(jobs) => jobs,
        Err(e) => {
            // Almost certainly the statement timeout: a rare state on a large
            // partition is a seq scan (measured at 4,634 buffers for 20 rows in
            // 200k). Saying so beats an empty table that looks like "no jobs".
            return patch(format!(
                r#"<div id="jobs"><h2>jobs</h2><p class="warn">this listing timed out — a rare state on a large queue has no index to use, so it reads most of the partition. Narrow the filter, or query it directly.</p><p class="dim">{}</p></div>"#,
                esc(&e.to_string())
            ));
        }
    };

    let mut html = format!(
        r#"<div id="jobs"><h2>jobs <span class="dim">— {q}, newest first</span></h2><nav>"#,
        q = esc(p.queue())
    );
    for s in ["", "available", "executing", "retryable", "discarded", "completed"] {
        let on = if p.state().unwrap_or("") == s { r#" class="on""# } else { "" };
        html.push_str(&format!(
            r#"<a{on} data-on-click="@get('{href}')">{label}</a>"#,
            href = p.link(None, Some(s), Some("")),
            label = if s.is_empty() { "all" } else { s },
        ));
    }
    html.push_str(r#"</nav><table><tr><th>id</th><th>task</th><th class="n">try</th><th>state</th><th>progress</th><th>error</th></tr>"#);

    if jobs.is_empty() {
        html.push_str(r#"<tr><td colspan="6" class="dim">nothing here</td></tr>"#);
    }
    for j in jobs {
        let state_class = match j.state.as_str() {
            "discarded" | "cancelled" => "bad",
            "retryable" => "warn",
            "executing" => "ok",
            _ => "dim",
        };
        html.push_str(&format!(
            r#"<tr><td><a data-on-click="@get('{href}')" class="dim">{id}</a></td><td>{task}</td><td class="n">{a}/{m}</td><td class="{state_class}">{st}</td><td class="dim">{progress}</td><td class="wide dim">{err}</td></tr>"#,
            href = p.link(None, None, Some(&j.id.to_string())),
            id = &j.id.to_string()[..8],
            task = esc(&j.task),
            a = j.attempt,
            m = j.max_attempts,
            st = esc(j.state.as_str()),
            progress = describe_progress(&j),
            err = esc(j.last_error.as_deref().unwrap_or("")),
        ));
    }
    html.push_str("</table></div>");
    patch(html)
}

/// One job, expanded: its failure history and its remaining retry schedule.
///
/// The two surfaces plans/06 asked for and did not have. Both render data that
/// was already on the row — the `errors` ring buffer, and the backoff policy
/// columns, which exist precisely so this can be shown WITHOUT re-deriving a
/// formula the dashboard would then have to keep in sync with the worker.
async fn render_job(state: &ApiState, p: &Params) -> Option<Event<PatchElements>> {
    let Some(raw) = p.job() else {
        return patch(r#"<div id="job"></div>"#.to_string());
    };
    let Ok(id) = raw.parse::<uuid::Uuid>() else {
        return patch(r#"<div id="job"></div>"#.to_string());
    };
    let Ok(Some(j)) = state.queue.fetch(p.queue(), id).await else {
        return patch(format!(
            r#"<div id="job"><h2>job</h2><p class="dim">{id} is not in {q} — it may have been pruned</p></div>"#,
            q = esc(p.queue())
        ));
    };

    let mut html = format!(
        r#"<div id="job"><h2>job {id} <a class="dim" data-on-click="@get('{back}')">[close]</a></h2><table>"#,
        back = p.link(None, None, Some("")),
    );

    html.push_str(&format!(
        r#"<tr><th>task</th><td>{task}</td><th>state</th><td class="{sc}">{st}</td></tr>
           <tr><th>attempt</th><td>{a} of {m}</td><th>interruptions</th><td>{i} of {mi}</td></tr>"#,
        task = esc(&j.task),
        sc = match j.state.as_str() {
            "discarded" | "cancelled" => "bad",
            "retryable" => "warn",
            _ => "dim",
        },
        st = esc(j.state.as_str()),
        a = j.attempt,
        m = j.max_attempts,
        i = j.interruptions,
        mi = j.max_interruptions,
    ));

    if j.deferrals > 0 {
        // Neither attempts nor interruptions: how many times WE declined to
        // run it. A high count means it is waiting on a sick dependency.
        html.push_str(&format!(
            r#"<tr><th>deferred</th><td colspan="3">{d} of {md} <span class="dim">— we declined to run it; no attempt charged</span></td></tr>"#,
            d = j.deferrals,
            md = j.max_deferrals,
        ));
    }
    if let Some(d) = &j.destination {
        html.push_str(&format!(r#"<tr><th>destination</th><td colspan="3">{}</td></tr>"#, esc(d)));
    }
    if let Some(src) = &j.source_queue {
        html.push_str(&format!(
            r#"<tr><th>dead letter</th><td colspan="3">from {} after {} attempt(s)</td></tr>"#,
            esc(src),
            j.source_attempts.unwrap_or(0)
        ));
    }
    if !describe_progress(&j).is_empty() {
        html.push_str(&format!(
            r#"<tr><th>progress</th><td colspan="3">{}</td></tr>"#,
            describe_progress(&j)
        ));
    }
    html.push_str("</table>");

    // The remaining retry schedule, from the policy ON THE ROW.
    //
    // underway's shape (plans/15): persisting the policy is what lets this be
    // rendered without the dashboard re-implementing the backoff formula and
    // then drifting from the worker's. Shown as the CEILING, because the
    // worker uses full jitter -- rand(0, ceiling) -- so any given retry lands
    // somewhere below this. Saying "in 4m" when the truth is "in up to 4m"
    // would be a lie an operator would plan around.
    if !j.state.is_terminal() && j.attempt < j.max_attempts {
        html.push_str(r#"<h2>retry schedule <span class="dim">— upper bound; the worker jitters below it</span></h2><table><tr><th>attempt</th><th>at most</th></tr>"#);
        for n in j.attempt..j.max_attempts.min(j.attempt + 6) {
            let ceiling = (j.backoff_base_ms as u64)
                .saturating_mul(2u64.saturating_pow(n.max(1) as u32 - 1))
                .min(j.backoff_max_ms as u64);
            html.push_str(&format!(
                r#"<tr><td>{}</td><td class="dim">{}</td></tr>"#,
                n + 1,
                format_age(ceiling as f64 / 1000.0)
            ));
        }
        html.push_str("</table>");
    }

    // The failure history: the structured ring buffer, not a formatted blob.
    // This is what distinguishes a flaky job from a consistently broken one at
    // a glance -- River bounds attempted_by but appends errors unbounded, so it
    // cannot show this without unbounded growth.
    if let Some(errors) = j.errors.as_array().filter(|e| !e.is_empty()) {
        html.push_str(r#"<h2>history</h2><table><tr><th>attempt</th><th>kind</th><th>when</th><th>error</th></tr>"#);
        for e in errors {
            let exit = match e["exit_status"].as_i64() {
                // The single highest-signal artefact in the survey, and thrown
                // away by everyone else.
                Some(137) => " [exit 137 = OOM]".to_string(),
                Some(c) => format!(" [exit {c}]"),
                None => String::new(),
            };
            html.push_str(&format!(
                r#"<tr><td class="n">{a}</td><td>{k}</td><td class="dim">{at}</td><td class="wide dim">{m}{exit}</td></tr>"#,
                a = e["attempt"].as_i64().unwrap_or(0),
                k = esc(e["kind"].as_str().unwrap_or("?")),
                at = esc(&e["at"].as_str().unwrap_or("").chars().skip(11).take(8).collect::<String>()),
                m = esc(e["message"].as_str().unwrap_or("")),
            ));
        }
        html.push_str("</table>");
    }

    html.push_str("</div>");
    patch(html)
}

/// Resume state, rendered for a human.
///
/// The checkpoint is opaque JSON by design — it is the job's own cursor and we
/// cannot know which field means "progress". So the SHAPE is reported rather
/// than guessed at: step names when it looks like a step record, otherwise the
/// raw cursor, truncated.
fn describe_progress(job: &crate::Job) -> String {
    let Some(cp) = &job.checkpoint else {
        return String::new();
    };

    if let Some(done) = cp.get("completed").and_then(|c| c.as_array()) {
        let names: Vec<_> = done.iter().filter_map(|s| s.as_str()).collect();
        let cursors = cp
            .get("cursors")
            .and_then(|c| c.as_object())
            .filter(|m| !m.is_empty())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();

        return esc(&format!(
            "steps: {}{}{}",
            names.join(","),
            if cursors.is_empty() { "" } else { " · " },
            cursors
        ));
    }

    let raw = cp.to_string();
    esc(&raw[..raw.len().min(48)])
}

async fn render_health(state: &ApiState) -> Option<Event<PatchElements>> {
    // Autovacuum falling behind never raises an error; it just makes everything
    // slower, which presents as "the queue is mysteriously slow" and sends
    // people reading their job code.
    let behind: Vec<_> = state
        .queue
        .vacuum_health()
        .await
        .ok()?
        .into_iter()
        .filter(|r| r.needs_vacuum)
        .collect();

    let workers: Vec<(String, i32, f64)> = sqlx::query_as(
        "SELECT hostname, pid, extract(epoch from now() - last_heartbeat)::float8 \
           FROM stampede_workers ORDER BY last_heartbeat",
    )
    .fetch_all(state.queue.pool())
    .await
    .unwrap_or_default();

    let mut html = String::from(r#"<div id="health">"#);

    if !behind.is_empty() {
        html.push_str(r#"<h2 class="bad">autovacuum is behind</h2><table>"#);
        for r in behind {
            html.push_str(&format!(
                r#"<tr><td>{t}</td><td class="n">{d} dead</td><td class="dim">threshold {th:.0}</td></tr>"#,
                t = esc(&r.relname),
                d = r.dead_tuples,
                th = r.autovac_threshold,
            ));
        }
        html.push_str("</table>");
    }

    html.push_str(r#"<h2>workers</h2><table><tr><th>worker</th><th class="n">heartbeat</th></tr>"#);
    if workers.is_empty() {
        html.push_str(r#"<tr><td colspan="2" class="dim">none registered</td></tr>"#);
    }
    for (host, pid, age) in workers {
        // Flagged past the reaper's default dead_after: this worker's jobs are
        // about to be reclaimed, which is worth seeing before it happens.
        let class = if age > 300.0 { "bad" } else { "dim" };
        html.push_str(&format!(
            r#"<tr><td>{h}:{pid}</td><td class="n {class}">{age}</td></tr>"#,
            h = esc(&host),
            age = format_age(age),
        ));
    }
    html.push_str("</table></div>");
    patch(html)
}

/// Compact age: "3847s" is harder to read at a glance than "1h4m".
fn format_age(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m{}s", s / 60, s % 60),
        _ => format!("{}h{}m", s / 3600, (s % 3600) / 60),
    }
}

fn patch(html: String) -> Option<Event<PatchElements>> {
    // COLLAPSED TO ONE LINE FIRST.
    //
    // SSE is line-oriented: a newline inside the payload splits the frame
    // across several `data:` lines. That is legal, and Datastar reassembles it
    // — but the panels here are built with multi-line Rust string literals, so
    // the indentation in the source was being shipped to every client every
    // second and the correctness of each frame depended on a reassembly step
    // nothing here can test without a browser. Collapsing removes both the
    // doubt and the bytes.
    let html = html
        .split('\n')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ");

    // NonEmptyStr makes "nothing to patch" a type-level concern rather than a
    // malformed event that would break the stream.
    let elements = rama::utils::str::NonEmptyStr::try_from(html).ok()?;
    PatchElements::new(elements)
        .with_mode(ElementPatchMode::Outer)
        .try_into_sse_event()
        .ok()
}

/// Escape for HTML.
///
/// Queue, task and host names are operator-controlled, but ERROR TEXT IS NOT —
/// it contains whatever a failing library put in a message, which can include
/// anything an attacker got into a request. It reaches the browser, so it is
/// escaped. So is the token, which is echoed into the stream URL and is
/// attacker-chosen by construction.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests;
