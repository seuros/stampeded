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
        .replace("__S__", "");
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
    let p = Params { token: None, q: Some(String::new()), s: Some(String::new()) };
    assert_eq!(p.queue(), "default");
    assert_eq!(p.state(), None);
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
