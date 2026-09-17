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

use crate::ApiState;
use rama::futures::stream;
use rama::http::header;
use rama::http::sse::datastar::{ElementPatchMode, PatchElements};
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use rama::http::service::web::extract::State;
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
<body data-on-load="@get('/dashboard/stream')">
  <h1>Stampede</h1>
  <div id="queues">connecting…</div>
  <div id="workers"></div>
  <footer>live via server-sent events</footer>
</body>
</html>"##;

pub async fn index() -> impl IntoResponse {
    Html(INDEX)
}

/// The live stream.
///
/// Polls the database on an interval and patches two fragments. Polling rather
/// than LISTEN/NOTIFY here is deliberate: a dashboard is allowed to be a second
/// stale, and a NOTIFY-per-job-transition would put dashboard load on the same
/// channel the workers depend on for wakeups (plans/12).
pub async fn stream(State(state): State<ApiState>) -> impl IntoResponse {
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

            let mut next: Vec<_> = [render_workers(&state).await, render_queues(&state).await]
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
}

async fn render_queues(state: &ApiState) -> Option<Event<PatchElements>> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT queue, state::text, count(*) \
           FROM stampede_jobs WHERE finished_at IS NULL \
          GROUP BY 1, 2 ORDER BY 1, 2",
    )
    .fetch_all(state.queue.pool())
    .await
    .ok()?;

    let mut html = String::from(
        r#"<div id="queues"><table><tr><th>queue</th><th>state</th><th class="n">jobs</th></tr>"#,
    );
    if rows.is_empty() {
        html.push_str(r#"<tr><td colspan="3">no unfinished jobs</td></tr>"#);
    }
    for (queue, st, n) in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{n}</td></tr>",
            esc(&queue),
            esc(&st)
        ));
    }
    html.push_str("</table></div>");

    patch(html)
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
