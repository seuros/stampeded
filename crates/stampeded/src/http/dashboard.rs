//! The operator console, served with Datastar over SSE. Off unless a token is set.
//!
//! It refreshes every second: no query here may aggregate the job table unbounded.

use super::{ApiState, trends};
use crate::JobState;
use crate::http::api::WakeupStatus;
use rama::futures::stream;
use rama::http::header;
use rama::http::service::web::extract::{Query, State};
use rama::http::service::web::response::{Html, IntoResponse, Sse};
use rama::http::sse::Event;
use rama::http::sse::datastar::{ElementPatchMode, PatchElements};
use rama::http::sse::server::KeepAlive;
use rama::net::uri::util::percent_encoding as pct;
use serde::Deserialize;
use std::convert::Infallible;
// `let _ = write!` into a String below: String's fmt::Write never errors.
use std::fmt::Write as _;
use std::time::{Duration, Instant};
use tracing::warn;

/// How far back the destination and failure panels look.
const WINDOW: Duration = Duration::from_secs(3600);

/// How long a shared panel render is reused; matches the tick.
const PANEL_TTL: Duration = Duration::from_secs(1);

/// Panels identical for every client, rendered once and shared.
///
/// Never `queues`, `jobs` or `job`: their links carry the client's token.
const SHARED: &[&str] = &["destinations", "failures", "schedules", "health", "tenants"];

/// How long the tenant panel is reused; it groups live job rows, not a counter.
const TENANT_TTL: Duration = Duration::from_secs(5);

/// Rows in the job list; see `Queue::recent_jobs` for its cost.
const JOB_LIMIT: i64 = 40;

const INDEX: &str = include_str!("dashboard/index.html");
const STYLE: &str = include_str!("dashboard/style.css");
const QUERY_VALUE: &pct::AsciiSet = &pct::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Dashboard credentials and filters.
///
/// The token is in the query string because `EventSource` cannot set headers.
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
        self.q
            .as_deref()
            .filter(|q| !q.is_empty())
            .unwrap_or("default")
    }

    fn state(&self) -> Option<&str> {
        self.s.as_deref().filter(|s| !s.is_empty())
    }

    fn job(&self) -> Option<&str> {
        self.j.as_deref().filter(|j| !j.is_empty())
    }

    /// Query string for an HTML attribute. Encode URL values before adding
    /// HTML-escaped separators; safe inside a Datastar string literal too.
    fn query(&self, q: Option<&str>, s: Option<&str>, job: &str) -> String {
        let token = self.token.as_deref().unwrap_or("");
        let queue = q.unwrap_or_else(|| self.queue());
        let state = s.or_else(|| self.state()).unwrap_or("");
        format!(
            "token={}&amp;q={}&amp;s={}&amp;j={}",
            pct::utf8_percent_encode(token, QUERY_VALUE),
            pct::utf8_percent_encode(queue, QUERY_VALUE),
            pct::utf8_percent_encode(state, QUERY_VALUE),
            pct::utf8_percent_encode(job, QUERY_VALUE),
        )
    }

    /// A page URL with selected fields swapped. `Some("")` clears a field.
    fn link(&self, q: Option<&str>, st: Option<&str>, job: &str) -> String {
        format!("/dashboard?{}#job-browser", self.query(q, st, job))
    }
}

fn current(selected: bool) -> &'static str {
    if selected {
        r#" aria-current="true""#
    } else {
        ""
    }
}

fn render_index(p: &Params) -> String {
    let query = p.query(None, None, p.job().unwrap_or(""));
    INDEX
        .replace("__STYLE__", STYLE)
        .replace("__STREAM__", &format!("/dashboard/stream?{query}"))
}

pub async fn index(Query(p): Query<Params>) -> impl IntoResponse {
    (
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        Html(render_index(&p)),
    )
        .into_response()
}

/// The live stream, polled so dashboard load stays off the workers' NOTIFY channel.
pub async fn stream(State(state): State<ApiState>, Query(p): Query<Params>) -> impl IntoResponse {
    let ticks = stream::unfold(
        (state, p, Vec::<Event<PatchElements>>::new(), true),
        move |(state, p, mut pending, first)| async move {
            if let Some(ev) = pending.pop() {
                return Some((Ok::<_, Infallible>(ev), (state, p, pending, false)));
            }

            // The first tick renders immediately.
            if !first {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // Built in reverse so `pop` emits them top-down.
            let mut next: Vec<_> = [
                cached(&state, "health", PANEL_TTL, render_health(&state)).await,
                cached(&state, "schedules", PANEL_TTL, render_schedules(&state)).await,
                render_jobs(&state, &p).await,
                render_job(&state, &p).await,
                cached(&state, "tenants", TENANT_TTL, render_tenants(&state)).await,
                cached(&state, "failures", PANEL_TTL, render_failures(&state)).await,
                // In memory, no query: rendered per client, nothing to cache.
                patch(&render_trends(&state.trends.snapshot(), Instant::now())),
                cached(
                    &state,
                    "destinations",
                    PANEL_TTL,
                    render_destinations(&state),
                )
                .await,
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

/// Share one render of a global panel across every connected client.
async fn cached<F>(
    state: &ApiState,
    key: &'static str,
    ttl: Duration,
    f: F,
) -> Option<Event<PatchElements>>
where
    F: std::future::Future<Output = Option<String>>,
{
    debug_assert!(SHARED.contains(&key), "{key} is not a shared panel");
    let html = state.panels.get(key, ttl, f).await?;
    patch(&html)
}

/// How a wakeup status reads on the dashboard; no workers is not a fault.
pub(crate) fn wakeup_badge(status: WakeupStatus) -> (&'static str, &'static str) {
    match status {
        WakeupStatus::Live => ("ok", "wakeups live"),
        WakeupStatus::PollingOnly => ("warn", "polling only"),
        WakeupStatus::NotApplicable => ("dim", "no workers in this process"),
    }
}

fn render_wakeups(state: &ApiState) -> Option<Event<PatchElements>> {
    let (class, text) = wakeup_badge(state.wakeups);
    patch(&format!(
        r#"<span id="wakeups" class="badge {class}" role="status">
               {text}</span>"#
    ))
}

/// Datastar itself, served from the binary so the console works air-gapped.
///
/// rama's copy matches the `PatchElements` encoder that generates the events.
pub async fn script() -> impl IntoResponse {
    rama::http::service::web::response::DatastarScript::new()
}

/// The Datastar source map, which devtools request from datastar.js.
pub async fn script_map() -> impl IntoResponse {
    rama::http::service::web::response::DatastarSourceMap::new()
}

async fn render_queues(state: &ApiState, p: &Params) -> Option<Event<PatchElements>> {
    // The counter table, never count(*): a per-second aggregate would pin xmin.
    let rows: Result<Vec<(String, String, i64)>, _> = sqlx::query_as(
        "SELECT queue, state, n FROM stampeded_queue_counts WHERE n <> 0 ORDER BY queue, state",
    )
    .fetch_all(state.queue.pool())
    .await;
    let rows = logged("queues", rows)?;

    let ages = logged("queues", state.queue.oldest_available_age().await).unwrap_or_default();
    let paused = logged("queues", state.queue.paused_queues().await).unwrap_or_default();

    let mut html = String::from(
        r#"<section id="queues" class="widget" tabindex="-1">
             <h2>queues</h2>
             <p class="hint">Select a queue or state to browse its jobs.</p>
             <div class="t"><table><tr><th>queue</th><th>state</th>
             <th class="n">jobs</th><th class="n">oldest</th></tr>"#,
    );
    if rows.is_empty() {
        html.push_str(r#"<tr><td colspan="4" class="dim">no jobs</td></tr>"#);
    }
    for (queue, st, n) in rows {
        // Waiting states only: an old completed job is retention, not backlog.
        let age = if st == "available" || st == "retryable" {
            ages.iter()
                .find(|(q, _)| *q == queue)
                .map(|(_, a)| format_age(*a))
                .unwrap_or_default()
        } else {
            String::new()
        };
        let selected_queue = queue == p.queue();
        let is_paused = paused.contains(&queue);
        let _ = write!(
            html,
            r#"<tr><td><a{on} href="{queue_link}">{q}</a>{tag}</td>
                 <td><a{state_on} href="{state_link}"
                        aria-label="{st} jobs in {q}">{st}</a></td>
                 <td class="n">{n}</td><td class="n dim">{age}</td></tr>"#,
            on = current(selected_queue),
            queue_link = p.link(Some(&queue), None, ""),
            state_on = current(selected_queue && p.state() == Some(&st)),
            state_link = p.link(Some(&queue), Some(&st), ""),
            q = esc(&queue),
            tag = if is_paused {
                r#" <span class="warn">[paused]</span>"#
            } else {
                ""
            },
            st = esc(&st),
        );
    }
    html.push_str("</table></div></section>");
    patch(&html)
}

/// Destination health, inferred from job rows: breaker state lives in worker memory.
async fn render_destinations(state: &ApiState) -> Option<String> {
    let rows = logged("destinations", state.queue.destination_health(WINDOW).await)?;
    if rows.is_empty() {
        return Some(r#"<div id="destinations"></div>"#.to_string());
    }

    let mut html = String::from(
        r#"<div id="destinations" class="widget"><h2>destinations
             <span class="dim">· observed from job rows, last hour</span></h2>
             <div class="t"><table><tr><th>destination</th>
             <th class="n">refused</th><th class="n">throttled</th>
             <th class="n">failed</th><th class="n">waiting</th>
             <th>state</th></tr>"#,
    );
    for r in rows {
        let (class, label) = if r.refused > 0 {
            ("bad", "circuit open")
        } else if r.failed > 0 {
            ("warn", "failing")
        } else {
            ("dim", "-")
        };
        let _ = write!(
            html,
            r#"<tr><td>{d}</td><td class="n">{refused}</td><td class="n">{thr}</td><td class="n">{fail}</td><td class="n">{wait}</td><td class="{class}">{label}</td></tr>"#,
            d = esc(r.destination.as_deref().unwrap_or("-")),
            refused = r.refused,
            thr = r.throttled,
            fail = r.failed,
            wait = r.waiting,
        );
    }
    html.push_str("</table></div></div>");
    Some(html)
}

/// Live jobs per tenant, busiest first, with each one's share of the backlog.
async fn render_tenants(state: &ApiState) -> Option<String> {
    let rows = logged("tenants", state.queue.tenant_load(15).await)?;
    Some(tenant_panel(&rows))
}

fn tenant_panel(rows: &[crate::queue::TenantLoad]) -> String {
    if rows.is_empty() {
        return r#"<div id="tenants"></div>"#.to_string();
    }
    let backlog: i64 = rows.iter().map(|r| r.waiting).sum();
    let mut html = String::from(
        r#"<div id="tenants" class="widget"><h2>tenants
             <span class="dim">· live jobs, busiest first</span></h2>
             <div class="t"><table><tr><th>tenant</th><th class="n">waiting</th>
             <th class="n">running</th><th class="n">throttled</th>
             <th>share</th></tr>"#,
    );
    for r in rows {
        // Share of the shown backlog, as a bar up to 6rem wide.
        let share = if backlog > 0 {
            r.waiting as f64 / backlog as f64
        } else {
            0.0
        };
        let _ = write!(
            html,
            r#"<tr><td>{t}</td><td class="n">{w}</td><td class="n">{run}</td><td class="n {tc}">{th}</td><td><span class="bar" style="width:{bar:.2}rem"></span></td></tr>"#,
            t = esc(&r.tenant),
            w = r.waiting,
            run = r.running,
            tc = if r.throttled > 0 { "warn" } else { "dim" },
            th = r.throttled,
            bar = share * 6.0,
        );
    }
    html.push_str("</table></div></div>");
    html
}

async fn render_failures(state: &ApiState) -> Option<String> {
    let rows = logged("failures", state.queue.failure_clusters(8).await)?;
    if rows.is_empty() {
        return Some(r#"<div id="failures"></div>"#.to_string());
    }

    let mut html = String::from(
        r#"<div id="failures" class="widget"><h2>failures
             <span class="dim">· clustered by fingerprint</span></h2>
             <div class="t"><table><tr><th class="n">n</th><th>kind</th>
             <th>destination</th><th>error</th></tr>"#,
    );
    for r in rows {
        let kind = r.failure_kind.unwrap_or_default();
        // Coloured by who is at fault: infrastructure, us declining, or the job.
        let class = match kind.as_str() {
            "worker_exited" | "worker_missing" | "heartbeat_lost" => "warn",
            "breaker_open" | "throttled" => "dim",
            _ => "bad",
        };
        let _ = write!(
            html,
            r#"<tr><td class="n">{n}</td><td class="{class}">{kind}</td><td class="dim">{dest}</td><td class="wide dim">{err}</td></tr>"#,
            n = r.n,
            kind = esc(&kind),
            dest = esc(r.destination.as_deref().unwrap_or("")),
            err = esc(r.sample.as_deref().unwrap_or("")),
        );
    }
    html.push_str("</table></div></div>");
    Some(html)
}

async fn render_schedules(state: &ApiState) -> Option<String> {
    let rows = logged("schedules", state.queue.schedules().await)?;
    if rows.is_empty() {
        return Some(r#"<div id="schedules"></div>"#.to_string());
    }

    let mut html = String::from(
        r#"<div id="schedules" class="widget"><h2>schedules</h2>
             <div class="t"><table><tr><th>key</th><th>rule</th><th>task</th>
             <th>next</th><th>last</th></tr>"#,
    );
    for r in rows {
        let rule = match (&r.cron_expr, r.interval_secs) {
            (Some(c), _) => format!("{} {} ({})", esc(c), esc(&r.timezone), esc(&r.dst_policy)),
            (_, Some(n)) => format!("every {n}s"),
            _ => "-".into(),
        };
        let _ = write!(
            html,
            r#"<tr><td class="{cls}">{key}{tag}</td><td class="dim">{rule}</td><td>{task}</td><td class="dim">{next}</td><td class="dim">{last}</td></tr>"#,
            cls = if r.paused { "warn" } else { "" },
            key = esc(&r.key),
            tag = if r.paused { " [paused]" } else { "" },
            task = esc(&r.task),
            next = r.next_run_at.strftime("%H:%M:%S"),
            last = r
                .last_run_at
                .map_or_else(|| "never".into(), |t| t.strftime("%H:%M:%S").to_string()),
        );
    }
    html.push_str("</table></div></div>");
    Some(html)
}

async fn render_jobs(state: &ApiState, p: &Params) -> Option<Event<PatchElements>> {
    let mut html = format!(
        r#"<div id="jobs" class="widget"><h2>jobs
             <span class="dim">· {q}, newest first</span></h2>
             <nav class="actions" aria-label="Job state">"#,
        q = esc(p.queue())
    );
    for s in std::iter::once("").chain(JobState::ALL.map(JobState::as_str)) {
        let _ = write!(
            html,
            r#"<a{on} href="{href}">{label}</a>"#,
            on = current(p.state().unwrap_or("") == s),
            href = p.link(None, Some(s), ""),
            label = if s.is_empty() { "all" } else { s },
        );
    }
    html.push_str("</nav>");

    let jobs = match state
        .queue
        .recent_jobs(p.queue(), p.state(), JOB_LIMIT)
        .await
    {
        Ok(jobs) => jobs,
        Err(e) => {
            // Almost always the statement timeout: a rare state on a large partition seq-scans.
            let _ = write!(
                html,
                r#"<p class="warn">Could not load jobs.
                     Try another state or queue.</p>
                     <p class="dim">{}</p></div>"#,
                esc(&e.to_string())
            );
            return patch(&html);
        }
    };

    html.push_str(
        r#"<div class="t"><table><tr><th>task / id</th><th class="n">try</th>
             <th>state</th><th>progress</th><th>error</th></tr>"#,
    );

    if jobs.is_empty() {
        html.push_str(
            r#"<tr><td colspan="5" class="dim">
                 No jobs match this filter.</td></tr>"#,
        );
    }
    for j in jobs {
        let state_class = match j.state.as_str() {
            "discarded" | "cancelled" => "bad",
            "retryable" => "warn",
            "executing" => "ok",
            _ => "dim",
        };
        let id = j.id.to_string();
        let _ = write!(
            html,
            r#"<tr id="job-row-{id}"><td>
                 <a{on} href="{href}" title="Inspect job {id}">{task}
                 <span class="job-id">{short}</span></a></td>
                 <td class="n">{a}/{m}</td><td class="{state_class}">{st}</td>
                 <td class="wide dim">{progress}</td>
                 <td class="wide dim">{err}</td></tr>"#,
            on = current(p.job() == Some(&id)),
            href = p.link(None, None, &id),
            short = &id[..8],
            task = esc(&j.task),
            a = j.attempt,
            m = j.max_attempts,
            st = esc(j.state.as_str()),
            progress = describe_progress(&j),
            err = esc(j.last_error.as_deref().unwrap_or("")),
        );
    }
    html.push_str("</table></div></div>");
    patch(&html)
}

/// One job, expanded: its failure history and its remaining retry schedule.
async fn render_job(state: &ApiState, p: &Params) -> Option<Event<PatchElements>> {
    let Some(raw) = p.job() else {
        return patch(r#"<div id="job"></div>"#);
    };
    let mut html = format!(
        r#"<div id="job" class="widget"><div class="widget-head">
             <h2>Job details</h2>
             <a class="action" href="{back}">Close details</a>
             </div>"#,
        back = p.link(None, None, ""),
    );
    let Ok(id) = raw.parse::<uuid::Uuid>() else {
        html.push_str(r#"<p class="warn">Invalid job ID.</p></div>"#);
        return patch(&html);
    };
    let job = logged("job", state.queue.fetch(p.queue(), id).await);
    let Some(Some(j)) = job else {
        let _ = write!(
            html,
            r#"<p class="dim">{id} {message}</p></div>"#,
            message = if job.is_some() {
                let queue = esc(p.queue());
                format!("is not in {queue} — it may have been pruned.")
            } else {
                "could not be loaded.".to_string()
            },
        );
        return patch(&html);
    };

    let _ = write!(
        html,
        r#"<div class="t"><table><tr><th>id</th><td colspan="3">{id}</td></tr>
           <tr><th>task</th><td>{task}</td>
               <th>state</th><td class="{sc}">{st}</td></tr>
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
    );

    if j.deferrals > 0 {
        // How often we declined to run it, not failures.
        let _ = write!(
            html,
            r#"<tr><th>deferred</th><td colspan="3">{d} of {md} <span class="dim">· we declined to run it; no attempt charged</span></td></tr>"#,
            d = j.deferrals,
            md = j.max_deferrals,
        );
    }
    if let Some(d) = &j.destination {
        let _ = write!(
            html,
            r#"<tr><th>destination</th><td colspan="3">{}</td></tr>"#,
            esc(d)
        );
    }
    if let Some(src) = &j.source_queue {
        let _ = write!(
            html,
            r#"<tr><th>dead letter</th><td colspan="3">from {} after {} attempt(s)</td></tr>"#,
            esc(src),
            j.source_attempts.unwrap_or(0)
        );
    }
    if !describe_progress(&j).is_empty() {
        let _ = write!(
            html,
            r#"<tr><th>progress</th><td colspan="3">{}</td></tr>"#,
            describe_progress(&j)
        );
    }
    html.push_str("</table></div>");

    // Retry ceilings from the row's policy; full jitter lands each retry below them.
    if !j.state.is_terminal() && j.attempt < j.max_attempts {
        html.push_str(r#"<h2>retry schedule <span class="dim">· upper bound; the worker jitters below it</span></h2><div class="t"><table><tr><th>attempt</th><th>at most</th></tr>"#);
        for n in j.attempt..j.max_attempts.min(j.attempt + 6) {
            // unsigned_abs is exact: CHECKs keep the policy positive, `n.max(1) - 1` >= 0.
            let ceiling = u64::from(j.backoff_base_ms.unsigned_abs())
                .saturating_mul(2u64.saturating_pow(u32::from((n.max(1) - 1).unsigned_abs())))
                .min(u64::from(j.backoff_max_ms.unsigned_abs()));
            let _ = write!(
                html,
                r#"<tr><td>{}</td><td class="dim">{}</td></tr>"#,
                n + 1,
                format_age(Duration::from_millis(ceiling).as_secs_f64())
            );
        }
        html.push_str("</table></div>");
    }

    if let Some(errors) = j.errors.as_array().filter(|e| !e.is_empty()) {
        html.push_str(r#"<h2>history</h2><div class="t"><table><tr><th>attempt</th><th>kind</th><th>when</th><th>error</th></tr>"#);
        for e in errors {
            let exit = match e["exit_status"].as_i64() {
                Some(137) => " [exit 137 = OOM]".to_string(),
                Some(c) => format!(" [exit {c}]"),
                None => String::new(),
            };
            let _ = write!(
                html,
                r#"<tr><td class="n">{a}</td><td>{k}</td><td class="dim">{at}</td><td class="wide dim">{m}{exit}</td></tr>"#,
                a = e["attempt"].as_i64().unwrap_or(0),
                k = esc(e["kind"].as_str().unwrap_or("?")),
                at = esc(&e["at"]
                    .as_str()
                    .unwrap_or("")
                    .chars()
                    .skip(11)
                    .take(8)
                    .collect::<String>()),
                m = esc(e["message"].as_str().unwrap_or("")),
            );
        }
        html.push_str("</table></div>");
    }

    html.push_str("</div>");
    patch(&html)
}

/// Resume state: step names for a step record, otherwise the raw cursor, truncated.
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

    // Arbitrary user JSON: never cut inside a multi-byte character.
    esc(&truncate_chars(&cp.to_string(), 48))
}

/// Truncate to at most `max` BYTES without splitting a character.
fn truncate_chars(s: &str, max: usize) -> String {
    s[..s.floor_char_boundary(max)].to_string()
}

/// Unwrap a panel query, logging the failure; a failed panel skips this tick.
fn logged<T>(panel: &str, result: Result<T, impl std::fmt::Display>) -> Option<T> {
    result
        .inspect_err(|e| warn!(panel, error = %e, "dashboard query failed"))
        .ok()
}

async fn render_health(state: &ApiState) -> Option<String> {
    // A lagging autovacuum never errors; the queue just gets slower.
    let behind: Vec<_> = logged("health", state.queue.vacuum_health().await)?
        .into_iter()
        .filter(|r| r.needs_vacuum)
        .collect();

    let workers: Vec<(String, i32, f64, Option<serde_json::Value>)> = logged(
        "health",
        sqlx::query_as(
            "SELECT hostname, pid, extract(epoch from now() - last_heartbeat)::float8, \
                    metadata->'vitals' \
               FROM stampeded_workers ORDER BY last_heartbeat",
        )
        .fetch_all(state.queue.pool())
        .await,
    )
    .unwrap_or_default();

    let mut html = String::from(r#"<div id="health" class="widget">"#);

    if !behind.is_empty() {
        html.push_str(r#"<h2 class="bad">autovacuum is behind</h2><div class="t"><table>"#);
        for r in behind {
            let _ = write!(
                html,
                r#"<tr><td>{t}</td><td class="n">{d} dead</td><td class="dim">threshold {th:.0}</td></tr>"#,
                t = esc(&r.relname),
                d = r.dead_tuples,
                th = r.autovac_threshold,
            );
        }
        html.push_str("</table></div>");
    }

    // Not an error, but latency silently becomes the poll interval there.
    if let Some(silent) = logged("health", state.queue.partitions_without_wakeup().await)
        && !silent.is_empty()
    {
        html.push_str(
            r#"<h2 class="warn">partitions without a wakeup trigger</h2><div class="t"><table>"#,
        );
        for p in &silent {
            let _ = write!(
                html,
                r#"<tr><td>{}</td><td class="dim">created outside stampeded_create_queue; jobs here are polled, not woken</td></tr>"#,
                esc(p)
            );
        }
        html.push_str("</table></div>");
    }

    html.push_str(r#"<h2>workers</h2><div class="t"><table><tr><th>worker</th><th class="n">heartbeat</th></tr>"#);
    if workers.is_empty() {
        html.push_str(r#"<tr><td colspan="2" class="dim">none registered</td></tr>"#);
    }
    for (host, pid, age, vitals) in workers {
        // Past the reaper's default dead_after: its jobs are about to be reclaimed.
        let class = if age > 300.0 { "bad" } else { "dim" };
        // Host vitals short: alive and heartbeating, but claiming nothing new.
        let note = crate::admission::paused_note(vitals.as_ref())
            .map(|n| format!(r#" <span class="warn">{}</span>"#, esc(&n)))
            .unwrap_or_default();
        let _ = write!(
            html,
            r#"<tr><td>{h}:{pid}{note}</td><td class="n {class}">{age}</td></tr>"#,
            h = esc(&host),
            age = format_age(age),
        );
    }
    html.push_str("</table></div></div>");
    Some(html)
}

/// Throughput, depth and backlog age per queue as sparklines, from the sampler's memory.
fn render_trends(series: &[(String, Vec<trends::Point>)], now: Instant) -> String {
    if series.is_empty() {
        return r#"<div id="trends"></div>"#.to_string();
    }
    // Span the history that exists, at least a minute, not the full window.
    let span = series
        .iter()
        .filter_map(|(_, points)| points.first())
        .map(|p| now.duration_since(p.at).as_secs_f64())
        .fold(0.0_f64, f64::max)
        .clamp(60.0, trends::WINDOW.as_secs_f64());
    let mut html = format!(
        r#"<div id="trends" class="widget"><h2>trends
             <span class="dim">· last {}</span></h2>
             <div class="t"><table><tr><th>queue</th>
             <th class="n">done/s</th><th></th><th class="n">waiting</th>
             <th></th><th class="n">oldest</th><th></th></tr>"#,
        format_age(span),
    );
    for (queue, points) in series {
        let Some(latest) = points.last() else {
            continue;
        };
        let age = |p: &trends::Point| now.duration_since(p.at).as_secs_f64();
        let done: Vec<_> = points
            .iter()
            .filter_map(|p| Some((age(p), p.rate?)))
            .collect();
        let waiting: Vec<_> = points.iter().map(|p| (age(p), p.waiting as f64)).collect();
        let oldest: Vec<_> = points
            .iter()
            .map(|p| (age(p), p.oldest_secs.unwrap_or(0.0)))
            .collect();
        let _ = write!(
            html,
            r#"<tr><td>{q}</td><td class="n ok">{rate}</td><td class="ok">{done}</td><td class="n flow">{w}</td><td class="flow">{wait}</td><td class="n warn">{o}</td><td class="warn">{old}</td></tr>"#,
            q = esc(queue),
            rate = latest
                .rate
                .map_or_else(|| "-".to_string(), |r| format!("{r:.1}")),
            done = sparkline(&done, span),
            w = latest.waiting,
            wait = sparkline(&waiting, span),
            o = latest
                .oldest_secs
                .map_or_else(|| "-".to_string(), format_age),
            old = sparkline(&oldest, span),
        );
    }
    html.push_str("</table></div></div>");
    html
}

/// An inline SVG of `(seconds ago, value)` points across `span`, scaled to its own peak.
fn sparkline(points: &[(f64, f64)], span: f64) -> String {
    const W: f64 = 120.0;
    const H: f64 = 24.0;
    let peak = points
        .iter()
        .map(|&(_, v)| v)
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    let mut line = String::new();
    if points.len() >= 2 {
        for &(ago, value) in points {
            let x = W * (1.0 - (ago / span).clamp(0.0, 1.0));
            // Inset by a pixel so a flat line at the peak is not clipped.
            let y = (value / peak).mul_add(-(H - 2.0), H - 1.0);
            let _ = write!(line, "{x:.1},{y:.1} ");
        }
    }
    format!(
        r#"<svg class="spark" viewBox="0 0 {W} {H}" preserveAspectRatio="none"><polyline points="{}"/></svg>"#,
        line.trim_end()
    )
}

/// Compact age: "3847s" is harder to read at a glance than "1h4m".
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "whole seconds wanted; float-to-int `as` saturates and maps NaN to 0"
)]
fn format_age(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m{}s", s / 60, s % 60),
        _ => format!("{}h{}m", s / 3600, (s % 3600) / 60),
    }
}

fn patch(html: &str) -> Option<Event<PatchElements>> {
    // One line, so each frame is a single `data:` line without source indentation.
    let html = html
        .split('\n')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ");

    // An empty patch would be a malformed event.
    let elements = rama::utils::str::NonEmptyStr::try_from(html).ok()?;
    PatchElements::new(elements)
        .with_mode(ElementPatchMode::Outer)
        .try_into_sse_event()
        .ok()
}

/// Escape untrusted text for HTML.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests;
