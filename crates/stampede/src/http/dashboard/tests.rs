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
    assert_eq!(esc(r#"<script>alert(1)</script>"#), "&lt;script&gt;alert(1)&lt;/script&gt;");
    assert_eq!(esc(r#"a&b"c'd"#), "a&amp;b&quot;c&#39;d");
}

#[test]
fn the_token_cannot_break_out_of_the_stream_url() {
    // The token is echoed into a data-on-click attribute and is
    // attacker-chosen by construction -- they pick the query string.
    let nasty = esc(r#"'><script>alert(1)</script>"#);
    assert!(!nasty.contains('\''), "single quote must be escaped: {nasty}");
    assert!(!nasty.contains('<'), "angle bracket must be escaped: {nasty}");
}

#[test]
fn the_page_has_a_target_for_every_panel() {
    // If an id drifts from the one a render_* emits, that panel silently stops
    // updating and the page looks merely quiet.
    for id in ["wakeups", "queues", "destinations", "failures", "jobs", "schedules", "health"] {
        assert!(INDEX.contains(&format!(r#"id="{id}""#)), "no target for {id}");
    }
}

#[test]
fn the_placeholders_are_all_substituted() {
    let page = INDEX
        .replace("__TOKEN__", "t")
        .replace("__Q__", "mail")
        .replace("__S__", "")
        .replace("__J__", "");
    assert!(!page.contains("__"), "an unsubstituted placeholder would break the stream URL");
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
    assert!(same_queue_all_states.contains("s=&"), "state should be cleared");

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
    let ev = patch("<div id=\"x\">\n   <table>\n     <tr><td>a</td></tr>\n   </table>\n</div>".to_string())
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
