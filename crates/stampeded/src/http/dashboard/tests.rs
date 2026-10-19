//! Rendering and escaping; the live behaviour is covered by tests/wire.rs.

use super::*;

#[test]
fn error_text_is_escaped() {
    // Error text can carry anything an attacker got into a request.
    assert_eq!(
        esc(r"<script>alert(1)</script>"),
        "&lt;script&gt;alert(1)&lt;/script&gt;"
    );
    assert_eq!(esc(r#"a&b"c'd"#), "a&amp;b&quot;c&#39;d");
}

#[test]
fn the_token_cannot_break_out_of_the_stream_url() {
    // The token is echoed into a data-on-click attribute and is attacker-chosen.
    let nasty = esc(r"'><script>alert(1)</script>");
    assert!(
        !nasty.contains('\''),
        "single quote must be escaped: {nasty}"
    );
    assert!(
        !nasty.contains('<'),
        "angle bracket must be escaped: {nasty}"
    );
}

#[test]
fn the_page_has_a_target_for_every_panel() {
    // A drifted id makes its panel silently stop updating.
    for id in [
        "wakeups",
        "queues",
        "destinations",
        "failures",
        "jobs",
        "schedules",
        "health",
    ] {
        assert!(
            INDEX.contains(&format!(r#"id="{id}""#)),
            "no target for {id}"
        );
    }
}

#[test]
fn the_placeholders_are_all_substituted() {
    let page = INDEX
        .replace("__TOKEN__", "t")
        .replace("__Q__", "mail")
        .replace("__S__", "")
        .replace("__J__", "");
    assert!(
        !page.contains("__"),
        "an unsubstituted placeholder would break the stream URL"
    );
    assert!(page.contains("token=t"));
    assert!(page.contains("q=mail"));
}

#[test]
fn params_default_to_the_default_queue_and_no_state_filter() {
    let p = Params::default();
    assert_eq!(p.queue(), "default");
    assert_eq!(p.state(), None);

    // Empty strings come from the "all" link and mean no filter.
    let p = Params {
        token: None,
        q: Some(String::new()),
        s: Some(String::new()),
        j: Some(String::new()),
    };
    assert_eq!(p.queue(), "default");
    assert_eq!(p.state(), None);
    assert_eq!(p.job(), None);
}

#[test]
fn a_link_swaps_one_field_and_keeps_the_rest() {
    let p = Params {
        token: Some("secret".into()),
        q: Some("mail".into()),
        s: Some("retryable".into()),
        j: None,
    };

    let same_queue_all_states = p.link(None, Some(""), None);
    assert!(same_queue_all_states.contains("token=secret"));
    assert!(same_queue_all_states.contains("q=mail"));
    assert!(
        same_queue_all_states.contains("s=&"),
        "state should be cleared"
    );

    // Opening a job keeps the filters, so [close] returns to the same list.
    let opened = p.link(None, None, Some("abc"));
    assert!(opened.contains("q=mail") && opened.contains("s=retryable"));
    assert!(opened.contains("j=abc"));
}

#[test]
fn ages_are_readable_at_a_glance() {
    assert_eq!(format_age(5.0), "5s");
    assert_eq!(format_age(90.0), "1m30s");
    assert_eq!(format_age(3847.0), "1h4m");
    // Clock skew can make this negative.
    assert_eq!(format_age(-1.0), "0s");
}

#[test]
fn patch_rejects_empty_elements() {
    assert!(patch("").is_none());
    assert!(patch("<div id=\"queues\">x</div>").is_some());
}

#[test]
fn progress_renders_steps_and_raw_cursors_differently() {
    use crate::{Job, JobState};

    let base = |checkpoint| Job {
        id: uuid::Uuid::now_v7(),
        queue: "default".into(),
        tenant_id: None,
        task: "t".into(),
        args: serde_json::json!({}),
        meta: serde_json::json!({}),
        state: JobState::Executing,
        priority: 0,
        scheduled_at: jiff::Timestamp::now(),
        attempt: 1,
        max_attempts: 25,
        attempted_by: vec![],
        claimed_by: None,
        claimed_at: None,
        dedup_scope: None,
        dedup_period: None,
        destination: None,
        interruptions: 0,
        max_interruptions: 1,
        deferrals: 0,
        max_deferrals: 100,
        expire_seconds: 3600,
        heartbeat_at: None,
        backoff_base_ms: 1000,
        backoff_max_ms: 3_600_000,
        checkpoint,
        checkpoint_at: None,
        checkpoint_seq: 0,
        failure_kind: None,
        last_error: None,
        errors: serde_json::json!([]),
        error_fingerprint: None,
        dead_letter_queue: None,
        source_id: None,
        source_queue: None,
        source_attempts: None,
        inserted_at: jiff::Timestamp::now(),
        finished_at: None,
    };

    assert_eq!(describe_progress(&base(None)), "");

    // A step record is summarised by name.
    let steps = serde_json::json!({
        "v": 1, "completed": ["charge", "render"], "cursors": { "email": 2 }
    });
    let out = describe_progress(&base(Some(steps)));
    assert!(out.contains("charge,render"), "{out}");
    assert!(out.contains("email=2"), "{out}");

    // A bare cursor is opaque, so it is shown raw and truncated.
    let raw = describe_progress(&base(Some(serde_json::json!({ "cursor": 50_000 }))));
    assert!(raw.contains("50000"), "{raw}");
}

#[test]
fn a_patch_is_always_one_sse_line() {
    let ev = patch("<div id=\"x\">\n   <table>\n     <tr><td>a</td></tr>\n   </table>\n</div>")
        .expect("should build");

    let rendered = format!("{ev:?}");
    assert!(
        !rendered.contains("\\n"),
        "a patch must not contain a newline: {rendered}"
    );
}

#[test]
fn collapsing_does_not_run_words_together() {
    // Joining with "" would turn `one\ntwo` in a text node into "onetwo".
    let ev = patch("<div id=\"x\">one\ntwo</div>").expect("should build");
    assert!(format!("{ev:?}").contains("one two"));
}

#[test]
fn no_shared_panel_can_contain_a_token() {
    // A shared panel embedding a token would hand one operator's credential to another.
    for panel in SHARED {
        assert!(
            !matches!(*panel, "queues" | "jobs" | "job"),
            "{panel} embeds the client's token in its links and must not be shared"
        );
    }
}

#[tokio::test]
async fn a_shared_panel_is_rendered_once_for_many_clients() {
    use crate::http::api::PanelCache;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let cache = PanelCache::default();
    let renders = AtomicUsize::new(0);

    for _ in 0..10 {
        let html = cache
            .get("failures", Duration::from_secs(60), async {
                renders.fetch_add(1, Ordering::SeqCst);
                Some("<div id=\"failures\">x</div>".to_string())
            })
            .await;
        assert!(html.is_some());
    }

    assert_eq!(renders.load(Ordering::SeqCst), 1, "ten clients, one render");
}

#[tokio::test]
async fn a_stale_panel_is_re_rendered() {
    use crate::http::api::PanelCache;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let cache = PanelCache::default();
    let renders = AtomicUsize::new(0);
    let render = || async {
        renders.fetch_add(1, Ordering::SeqCst);
        Some("<div id=\"health\">x</div>".to_string())
    };

    cache
        .get("health", Duration::from_millis(30), render())
        .await;
    cache
        .get("health", Duration::from_millis(30), render())
        .await;
    assert_eq!(renders.load(Ordering::SeqCst), 1, "still fresh");

    tokio::time::sleep(Duration::from_millis(50)).await;
    cache
        .get("health", Duration::from_millis(30), render())
        .await;
    assert_eq!(renders.load(Ordering::SeqCst), 2, "expired, so re-rendered");
}

/// A healthy `--no-worker` daemon must not be reported as degraded.
#[test]
fn no_worker_daemon_is_not_reported_as_degraded() {
    let (class, text) = wakeup_badge(WakeupStatus::NotApplicable);
    assert_eq!(
        class, "dim",
        "a daemon with no workers is not a warning: {text}"
    );
    assert_ne!(class, "warn");

    // The genuinely degraded case must still be amber.
    assert_eq!(wakeup_badge(WakeupStatus::PollingOnly).0, "warn");
    assert_eq!(wakeup_badge(WakeupStatus::Live).0, "ok");

    let texts = [
        wakeup_badge(WakeupStatus::Live).1,
        wakeup_badge(WakeupStatus::PollingOnly).1,
        wakeup_badge(WakeupStatus::NotApplicable).1,
    ];
    let unique: std::collections::BTreeSet<_> = texts.iter().collect();
    assert_eq!(
        unique.len(),
        3,
        "wakeup states must be distinguishable: {texts:?}"
    );
}

/// A checkpoint containing non-ASCII must not panic the job panel.
#[test]
fn a_checkpoint_with_multibyte_characters_does_not_panic() {
    // Byte 48 falls inside the 'é'.
    let raw = r#"{"file":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxé.pdf"}"#;
    assert!(
        raw.len() > 48 && !raw.is_char_boundary(48),
        "fixture no longer exercises the bug"
    );

    let out = truncate_chars(raw, 48);

    assert!(out.len() <= 48);
    assert!(raw.starts_with(&out), "truncation must be a prefix");
    assert_eq!(out, String::from_utf8(out.clone().into_bytes()).unwrap());
}

/// Every byte offset must be safe.
#[test]
fn truncation_is_safe_at_every_offset() {
    let s = "héllo wörld -- ünïcödé ✓ 日本語 🎉";
    for n in 0..=s.len() + 4 {
        let out = truncate_chars(s, n);
        assert!(s.starts_with(&out), "not a prefix at {n}");
        assert!(out.len() <= n || s.len() <= n, "too long at {n}");
    }
}

/// The operator console must not depend on the internet.
#[test]
fn the_dashboard_loads_no_external_assets() {
    use crate::http::test_support::network_fetches;
    let html = INDEX;

    // The scanner itself: a CDN script counts, an XML namespace does not.
    assert_eq!(
        network_fetches(
            r#"<script src="https://cdn.example/x.js"></script><svg xmlns="http://www.w3.org/2000/svg">"#
        ),
        ["https://cdn.example/x.js"]
    );
    assert_eq!(
        network_fetches(html),
        Vec::<String>::new(),
        "the dashboard must serve its own assets"
    );

    assert!(
        html.contains("/dashboard/datastar.js"),
        "the dashboard does not load Datastar at all"
    );
}

/// Every `data-*` attribute on the page must be a real Datastar plugin.
///
/// The list is from the Datastar source (`library/src/plugins/attributes/`).
#[test]
fn every_datastar_attribute_on_the_page_exists() {
    const PLUGINS: &[&str] = &[
        "attr",
        "bind",
        "class",
        "computed",
        "effect",
        "indicator",
        "init",
        "json-signals",
        "on",
        "on-intersect",
        "on-interval",
        "on-signal-patch",
        "ref",
        "show",
        "signals",
        "style",
        "text",
    ];

    let mut seen = 0;
    for (i, _) in INDEX.match_indices("data-") {
        let rest = &INDEX[i + 5..];
        let attr: String = rest
            .chars()
            .take_while(|c| c.is_ascii_lowercase() || *c == '-')
            .collect();
        if attr.is_empty() {
            continue;
        }
        seen += 1;

        // `on-<event>` is the `on` plugin with a DOM event; anything else must be a plugin.
        let ok = PLUGINS.contains(&attr.as_str())
            || attr.strip_prefix("on-").is_some_and(|ev| {
                !ev.is_empty() && !PLUGINS.contains(&format!("on-{ev}").as_str())
                    || PLUGINS.contains(&format!("on-{ev}").as_str())
            });

        assert!(ok, "data-{attr} is not a Datastar attribute plugin");
        // `load` never fires on <body>.
        assert_ne!(attr, "on-load", "data-on-load never fires; use data-init");
    }
    assert!(seen > 0, "the page uses no Datastar attributes at all");
}

#[test]
fn a_sparkline_spans_the_window_and_peaks_at_the_top() {
    let svg = sparkline(&[(300.0, 0.0), (150.0, 5.0), (0.0, 10.0)], 300.0);
    // Oldest bottom-left, newest top-right, inset one unit from the edge.
    assert!(
        svg.contains(r#"points="0.0,23.0 60.0,12.0 120.0,1.0""#),
        "{svg}"
    );

    // One point has no shape; the box is still drawn so the row keeps its width.
    let lone = sparkline(&[(0.0, 3.0)], 300.0);
    assert!(lone.contains(r#"points="""#), "{lone}");
}

#[test]
fn the_trends_panel_names_queues_safely_and_shows_the_latest_values() {
    use crate::http::trends::Point;
    let now = Instant::now();
    let at = |ago: u64| now.checked_sub(Duration::from_secs(ago)).unwrap();
    let series = vec![(
        "<mail>".to_string(),
        vec![
            Point {
                at: at(10),
                rate: None,
                waiting: 40,
                oldest_secs: Some(90.0),
            },
            Point {
                at: at(5),
                rate: Some(12.5),
                waiting: 30,
                oldest_secs: Some(65.0),
            },
        ],
    )];

    let html = render_trends(&series, now);
    assert!(
        html.contains("&lt;mail&gt;"),
        "queue names are escaped: {html}"
    );
    assert!(html.contains(">12.5<"), "latest rate: {html}");
    assert!(html.contains(">30<"), "latest waiting: {html}");
    assert!(html.contains(">1m5s<"), "latest oldest: {html}");

    assert!(
        html.contains("last 1m0s"),
        "ten seconds of history is drawn across the one-minute floor: {html}"
    );

    assert_eq!(render_trends(&[], now), r#"<div id="trends"></div>"#);
}

#[test]
fn the_tenant_panel_bars_each_tenants_share_of_the_backlog() {
    use crate::queue::TenantLoad;
    let row = |tenant: &str, waiting, running, throttled| TenantLoad {
        tenant: tenant.to_string(),
        waiting,
        running,
        throttled,
    };
    let html = tenant_panel(&[row("acme", 300, 4, 25), row("<globex>", 100, 1, 0)]);

    assert!(
        html.contains("width:4.50rem"),
        "acme holds 75% of the backlog: {html}"
    );
    assert!(html.contains("width:1.50rem"), "{html}");
    assert!(
        html.contains(r#"<td class="n warn">25</td>"#),
        "throttling is flagged: {html}"
    );
    assert!(
        html.contains("&lt;globex&gt;"),
        "tenant names are escaped: {html}"
    );

    // An empty table would read as "no tenants exist".
    assert_eq!(tenant_panel(&[]), r#"<div id="tenants"></div>"#);
}
