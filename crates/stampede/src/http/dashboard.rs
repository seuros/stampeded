//! Live dashboard, served with Datastar over SSE.
//!
//! A job dashboard is ~90% "stream state changes into a table", which is
//! exactly SSE's shape (plans/06). Sidekiq Web is ERB plus polling; GoodJob is
//! Rails plus Turbo; River UI is a React SPA with its own build pipeline. This
//! is a single HTML page and a server-sent event stream — no Node toolchain, no
//! bundler, nothing to `npm install`.
//!
//! Datastar patches elements by selector, so the server sends the rendered
//! fragment and the browser swaps it in. All rendering stays in Rust.

use super::ApiState;
use rama::futures::stream;
use rama::http::header;
use rama::http::sse::datastar::{ElementPatchMode, PatchElements};
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use rama::http::StatusCode;
use rama::http::service::web::extract::{Query, State};
use serde::Deserialize;
use rama::http::service::web::response::{Html, IntoResponse, Sse};
use std::convert::Infallible;
use std::time::Duration;

/// The shell. Loads Datastar from a CDN and opens one SSE stream; every update
/// after that is a patched fragment.
const INDEX: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Stampede</title>
<script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js"></script>
<style>
  :root { color-scheme: dark light }
  body { font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; margin: 2rem; }
  h1 { font-size: 1.1rem; letter-spacing: .08em; text-transform: uppercase; opacity: .7 }
  table { border-collapse: collapse; margin: 1rem 0 2rem }
  th, td { text-align: left; padding: .3rem 1.2rem .3rem 0 }
  th { font-weight: 600; opacity: .6; border-bottom: 1px solid currentColor }
  .n { text-align: right; font-variant-numeric: tabular-nums }
  .stale { color: #c0392b }
  footer { opacity: .5; font-size: .85em; margin-top: 2rem }
</style>
</head>
<body data-on-load="@get('/dashboard/stream?token=__TOKEN__')">
  <h1>Stampede</h1>
  <div id="health"></div>
  <div id="queues">connecting…</div>
  <div id="workers"></div>
  <footer>live via server-sent events</footer>
</body>
</html>"##;

/// The dashboard shell.
///
/// Token comes in on the query string because the page has to hand it to the
/// SSE stream, and EventSource cannot set headers. That puts the secret in
/// server logs and in browser history, which is the honest cost of a
/// dependency-free auth model — deployments that care should put this behind
/// their own proxy and not set a token at all.
pub async fn index(
    State(state): State<ApiState>,
    Query(auth): Query<Auth>,
) -> impl IntoResponse {
    if !state.dashboard_authorised(auth.token.as_deref()) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    // Templated into the stream URL so the browser can authenticate the SSE
    // connection too.
    Html(INDEX.replace("__TOKEN__", &encode_attr(auth.token.as_deref().unwrap_or(""))))
        .into_response()
}

/// Dashboard credentials.
#[derive(Debug, Deserialize)]
pub struct Auth {
    #[serde(default)]
    pub token: Option<String>,
}

/// Escape for an HTML attribute inside single quotes.
fn encode_attr(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('\'', "&#39;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
}

/// The live stream.
///
/// Polls the database on an interval and patches two fragments. Polling rather
/// than LISTEN/NOTIFY here is deliberate: a dashboard is allowed to be a second
/// stale, and a NOTIFY-per-job-transition would put dashboard load on the same
/// channel the workers depend on for wakeups (plans/12).
pub async fn stream(
    State(state): State<ApiState>,
    Query(auth): Query<Auth>,
) -> impl IntoResponse {
    if !state.dashboard_authorised(auth.token.as_deref()) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    // One tick emits at most two events (queues, workers), so the state carries
    // a small pending buffer rather than nesting a stream per tick — nested
    // streams need StreamExt::flatten and buy nothing here.
    let ticks = stream::unfold(
        (state, Vec::<Event<PatchElements>>::new()),
        move |(state, mut pending)| async move {
            if let Some(ev) = pending.pop() {
                return Some((Ok::<_, Infallible>(ev), (state, pending)));
            }

            tokio::time::sleep(Duration::from_secs(1)).await;

            let mut next: Vec<_> = [
                render_health(&state).await,
                render_workers(&state).await,
                render_queues(&state).await,
            ]
            .into_iter()
            .flatten()
            .collect();

            let first = next.pop()?;
            Some((Ok(first), (state, next)))
        },
    );

    (
        [(header::CACHE_CONTROL, "no-cache")],
        // Keep-alive comments stop proxies closing an idle stream. rama's
        // default interval is already 15s.
        Sse::new(ticks).with_keep_alive(KeepAlive::new()),
    )
        .into_response()
}

async fn render_queues(state: &ApiState) -> Option<Event<PatchElements>> {
    // Reads the COUNTER TABLE, never `count(*)` over the jobs table.
    //
    // This is the query that made the rule necessary: it runs once a second,
    // forever, for as long as anyone leaves the dashboard open. An unbounded
    // aggregate holds the xmin horizon, vacuum cannot reclaim below it, the
    // table bloats, the aggregate slows, and it holds the horizon longer --
    // pg-boss's documented death spiral, triggered by watching the dashboard.
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT queue, state, n FROM stampede_queue_counts \
          WHERE n <> 0 ORDER BY queue, state",
    )
    .fetch_all(state.queue.pool())
    .await
    .ok()?;

    // Age of the oldest waiting job. Depth says how much work there is; this
    // says whether it is being DONE. 10,000 jobs aged two seconds is healthy,
    // 3 jobs aged an hour is broken, and depth alone makes the second look
    // like the quieter of the two.
    let ages: Vec<(String, f64)> = state
        .queue
        .oldest_available_age()
        .await
        .unwrap_or_default();

    let mut html = String::from(
        r#"<div id="queues"><table><tr><th>queue</th><th>state</th><th class="n">jobs</th><th class="n">oldest</th></tr>"#,
    );
    if rows.is_empty() {
        html.push_str(r#"<tr><td colspan="4">no jobs</td></tr>"#);
    }
    for (queue, st, n) in rows {
        // Only against the states that are actually waiting: the oldest
        // COMPLETED job is just retention, not backlog.
        let age = if st == "available" || st == "retryable" {
            ages.iter()
                .find(|(q, _)| *q == queue)
                .map(|(_, a)| format_age(*a))
                .unwrap_or_default()
        } else {
            String::new()
        };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{n}</td><td class=\"n\">{age}</td></tr>",
            esc(&queue),
            esc(&st)
        ));
    }
    html.push_str("</table></div>");

    patch(html)
}

/// Compact age, because "3847s" is harder to read at a glance than "1h4m".
fn format_age(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m{}s", s / 60, s % 60),
        _ => format!("{}h{}m", s / 3600, (s % 3600) / 60),
    }
}

async fn render_workers(state: &ApiState) -> Option<Event<PatchElements>> {
    let rows: Vec<(String, i32, f64)> = sqlx::query_as(
        "SELECT hostname, pid, extract(epoch from now() - last_heartbeat)::float8 \
           FROM stampede_workers ORDER BY last_heartbeat",
    )
    .fetch_all(state.queue.pool())
    .await
    .ok()?;

    let mut html = String::from(
        r#"<div id="workers"><table><tr><th>worker</th><th class="n">heartbeat</th></tr>"#,
    );
    if rows.is_empty() {
        html.push_str(r#"<tr><td colspan="2">no workers registered</td></tr>"#);
    }
    for (host, pid, age) in rows {
        // Flagged past the reaper's default dead_after: a stale worker's jobs
        // are about to be reclaimed, which is worth seeing before it happens.
        let class = if age > 300.0 { " class=\"stale\"" } else { "" };
        html.push_str(&format!(
            "<tr{class}><td>{}:{pid}</td><td class=\"n\">{age:.0}s</td></tr>",
            esc(&host)
        ));
    }
    html.push_str("</table></div>");

    patch(html)
}

/// Autovacuum standing and the wakeup path.
///
/// Both are things that silently degrade rather than fail: a queue whose
/// notifications are eaten by a pooler runs a second late forever, and one whose
/// autovacuum is behind gets gradually slower. Neither produces an error
/// anywhere, so neither is discoverable without being shown.
async fn render_health(state: &ApiState) -> Option<Event<PatchElements>> {
    let mut html = String::from(r#"<div id="health"><table>"#);

    let wakeups = if state.notifications_live {
        r#"<td>live</td>"#
    } else {
        // Not an error -- the queue is correct either way -- but worth seeing
        // before someone spends an afternoon wondering about latency.
        r#"<td class="stale">polling only</td>"#
    };
    html.push_str(&format!("<tr><th>wakeups</th>{wakeups}</tr>"));

    if let Ok(rows) = state.queue.vacuum_health().await {
        for r in rows.into_iter().filter(|r| r.needs_vacuum) {
            html.push_str(&format!(
                "<tr><th>vacuum</th><td class=\"stale\">{} — {} dead tuples, past its \
                 threshold of {:.0}</td></tr>",
                esc(&r.relname),
                r.dead_tuples,
                r.autovac_threshold
            ));
        }
    }

    html.push_str("</table></div>");
    patch(html)
}

fn patch(html: String) -> Option<Event<PatchElements>> {
    // NonEmptyStr makes "nothing to patch" a type-level concern rather than a
    // malformed event that would break the stream.
    let elements = rama::utils::str::NonEmptyStr::try_from(html).ok()?;
    PatchElements::new(elements)
        .with_mode(ElementPatchMode::Outer)
        .try_into_sse_event()
        .ok()
}

/// Minimal HTML escaping. Queue names, hostnames and task names are
/// operator-controlled rather than end-user input, but they reach the browser,
/// so they get escaped anyway.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests;
