//! Rendering and escaping.
//!
//! The live behaviour needs a database and is covered by tests/wire.rs; this is
//! the string handling, which is where the XSS is.

use super::*;

#[test]
fn error_text_is_escaped() {
    // Queue and task names are operator-controlled, but ERROR TEXT is not: it
    // contains whatever a failing library put in a message, which can include
    // anything an attacker got into a request. It goes straight into the page.
    assert_eq!(
        esc(r#"<script>alert(1)</script>"#),
        "&lt;script&gt;alert(1)&lt;/script&gt;"
    );
    assert_eq!(esc(r#"a&b"c'd"#), "a&amp;b&quot;c&#39;d");
}

#[test]
fn the_token_cannot_break_out_of_the_stream_url() {
    // The token is echoed into a data-on-click attribute and is
    // attacker-chosen by construction -- they pick the query string.
    let nasty = esc(r#"'><script>alert(1)</script>"#);
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
    // If an id drifts from the one a render_* emits, that panel silently stops
    // updating and the page looks merely quiet.
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

    // Empty strings come from the "all" link and must not become a filter for
    // a queue literally named "".
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
    // Filter links are built by swapping a single field, so a stale token or a
    // lost queue filter would show up here rather than as a dashboard that
    // silently drops back to defaults on every click.
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
    // Clock skew between app and database can make this negative; it must not
    // panic on the cast.
    assert_eq!(format_age(-1.0), "0s");
}

#[test]
fn patch_rejects_empty_elements() {
    assert!(patch(String::new()).is_none());
    assert!(patch("<div id=\"queues\">x</div>".to_string()).is_some());
}

#[test]
fn progress_renders_steps_and_raw_cursors_differently() {
    use crate::{Job, JobState};
    use chrono::Utc;

    let base = |checkpoint| Job {
        id: uuid::Uuid::now_v7(),
        queue: "default".into(),
        tenant_id: None,
        task: "t".into(),
        args: serde_json::json!({}),
        state: JobState::Executing,
        priority: 0,
        scheduled_at: Utc::now(),
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
        inserted_at: Utc::now(),
        finished_at: None,
    };

    assert_eq!(describe_progress(&base(None)), "");

    // A step record is summarised by name, because that is what an operator
    // wants to know: which phases are done.
    let steps = serde_json::json!({
        "v": 1, "completed": ["charge", "render"], "cursors": { "email": 2 }
    });
    let out = describe_progress(&base(Some(steps)));
    assert!(out.contains("charge,render"), "{out}");
    assert!(out.contains("email=2"), "{out}");

    // A bare cursor is opaque to us -- we cannot know which field means
    // progress -- so it is shown raw and truncated.
    let raw = describe_progress(&base(Some(serde_json::json!({ "cursor": 50_000 }))));
    assert!(raw.contains("50000"), "{raw}");
}

#[test]
fn a_patch_is_always_one_sse_line() {
    // SSE is line-oriented: a newline in the payload splits the frame across
    // several `data:` lines. Legal, and Datastar reassembles it, but the panels
    // are built from multi-line Rust literals -- so without collapsing, source
    // indentation shipped to every client every second and each frame's
    // correctness depended on a reassembly step nothing here can test without a
    // browser.
    let ev = patch(
        "<div id=\"x\">\n   <table>\n     <tr><td>a</td></tr>\n   </table>\n</div>".to_string(),
    )
    .expect("should build");

    let rendered = format!("{ev:?}");
    assert!(
        !rendered.contains("\\n"),
        "a patch must not contain a newline: {rendered}"
    );
}

#[test]
fn collapsing_does_not_run_words_together() {
    // Joining with "" instead of " " would turn `<td>a</td>\n<td>b</td>` into
    // valid HTML but `one\ntwo` in a text node into "onetwo".
    let ev = patch("<div id=\"x\">one\ntwo</div>".to_string()).expect("should build");
    assert!(format!("{ev:?}").contains("one two"));
}

#[test]
fn no_shared_panel_can_contain_a_token() {
    // THE HAZARD THIS CONSTANT EXISTS FOR. Shared panels are rendered once and
    // served to every connected client, so a panel that embeds the requesting
    // client's token would hand one operator's credential to another.
    //
    // `queues`, `jobs` and `job` all build filter links carrying the token, so
    // they must stay per-client. They are also the cheap ones -- the four that
    // ARE shared are the expensive queries -- so this costs nothing.
    for panel in SHARED {
        assert!(
            !matches!(*panel, "queues" | "jobs" | "job"),
            "{panel} embeds the client's token in its links and must not be shared"
        );
    }
}

#[tokio::test]
async fn a_shared_panel_is_rendered_once_for_many_clients() {
    // Ten operators watching should cost what one does. Before the cache each
    // client re-ran every query every second for byte-identical output -- and a
    // dashboard gets opened by several people at once precisely during an
    // incident.
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
    // The cache must expire, or the dashboard stops being live -- which is the
    // one thing it is for.
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
///
/// This was a two-state flag, and a daemon with no worker role -- the
/// RECOMMENDED shape for Rails, where the workers are Ruby -- fell into the
/// `false` arm and showed amber "polling only" forever. Nothing was wrong: it
/// starts no listener because it has nothing to wake. A permanent amber badge
/// on a healthy system is how operators learn to ignore amber, so the
/// distinction is load-bearing rather than cosmetic.
#[test]
fn no_worker_daemon_is_not_reported_as_degraded() {
    let (class, text) = wakeup_badge(WakeupStatus::NotApplicable);
    assert_eq!(
        class, "dim",
        "a daemon with no workers is not a warning: {text}"
    );
    assert_ne!(class, "warn");

    // The genuinely degraded case must still be amber, or this fix would have
    // traded one silent state for another.
    assert_eq!(wakeup_badge(WakeupStatus::PollingOnly).0, "warn");
    assert_eq!(wakeup_badge(WakeupStatus::Live).0, "ok");

    // All three must read differently, so the badge distinguishes them.
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
///
/// `describe_progress` fell back to `&raw[..raw.len().min(48)]` for a
/// checkpoint that is not in our steps format. That is a BYTE index, and a
/// checkpoint is arbitrary user JSON -- a filename, a customer name, a city.
/// When byte 48 lands inside a multi-byte character, slicing panics, and it
/// panics inside the SSE stream rendering the detail panel for exactly the
/// kind of job an operator is looking at: a long one, with a checkpoint.
#[test]
fn a_checkpoint_with_multibyte_characters_does_not_panic() {
    // Byte 48 falls inside the 'é' here -- the case found by searching for it.
    let raw = r#"{"file":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxé.pdf"}"#;
    assert!(
        raw.len() > 48 && !raw.is_char_boundary(48),
        "fixture no longer exercises the bug"
    );

    let out = truncate_chars(raw, 48);

    assert!(out.len() <= 48);
    assert!(raw.starts_with(&out), "truncation must be a prefix");
    // Round-trips as valid UTF-8 by construction -- the point of the fix.
    assert_eq!(out, String::from_utf8(out.clone().into_bytes()).unwrap());
}

/// Every byte offset must be safe, not just the one that was reported.
#[test]
fn truncation_is_safe_at_every_offset() {
    let s = "héllo wörld — ünïcödé ✓ 日本語 🎉";
    for n in 0..=s.len() + 4 {
        let out = truncate_chars(s, n);
        assert!(s.starts_with(&out), "not a prefix at {n}");
        assert!(out.len() <= n || s.len() <= n, "too long at {n}");
    }
}
