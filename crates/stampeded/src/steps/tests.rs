//! The step record, and its compatibility with the Ruby writer.

use super::*;

#[test]
fn a_plain_cursor_checkpoint_is_not_mistaken_for_a_step_record() {
    // The two APIs share one column; a `checkpoint()` value has no `completed` key.
    let plain = serde_json::json!({ "cursor": 50_000 });
    let state = StepState::from_checkpoint(Some(&plain));

    assert!(state.completed.is_empty());
    assert!(state.values.is_empty());
    assert!(!state.completed("anything"));
}

#[test]
fn nothing_at_all_is_an_empty_record() {
    let state = StepState::from_checkpoint(None);
    assert!(state.completed.is_empty());
    assert_eq!(state.v, VERSION);
}

#[test]
fn a_record_written_by_the_ruby_worker_reads_back() {
    // Ruby writes this exact shape.
    let from_ruby = serde_json::json!({
        "v": 1,
        "completed": ["charge", "render"],
        "values": { "render": "/tmp/receipt.pdf" },
        "cursors": { "email": 2 }
    });

    let state = StepState::from_checkpoint(Some(&from_ruby));

    assert!(state.completed("charge"));
    assert!(state.completed("render"));
    assert!(
        !state.completed("email"),
        "a step with a cursor is in progress, not done"
    );
    assert_eq!(state.value::<String>("render").unwrap(), "/tmp/receipt.pdf");
    assert_eq!(state.cursor::<i64>("email").unwrap(), 2);
}

#[test]
fn what_rust_writes_is_what_ruby_expects() {
    // Otherwise a Ruby worker resuming a Rust checkpoint silently sees no steps.
    let mut state = StepState::default();
    state.complete("charge", None::<&()>).unwrap();
    state.complete("render", Some(&"/tmp/r.pdf")).unwrap();
    state.set_cursor("email", &2).unwrap();

    let json = serde_json::to_value(&state).unwrap();

    assert_eq!(json["v"], 1);
    assert_eq!(json["completed"], serde_json::json!(["charge", "render"]));
    assert_eq!(json["values"]["render"], "/tmp/r.pdf");
    assert_eq!(json["cursors"]["email"], 2);
    // A step run for its side effect stores no value.
    assert!(json["values"].get("charge").is_none());
}

#[test]
fn completing_a_step_clears_its_cursor() {
    let mut state = StepState::default();
    state.set_cursor("email", &7).unwrap();
    state.complete("email", None::<&()>).unwrap();

    assert!(state.cursors.is_empty());
    assert!(state.completed("email"));
}

#[test]
fn an_oversized_step_result_is_refused_with_advice() {
    // Dropping it silently would change behavior after the first crash.
    let mut state = StepState::default();
    let huge = "x".repeat(20_000);
    let err = state.complete("big", Some(&huge)).unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("20"), "should name the size: {msg}");
    assert!(
        msg.contains("reference"),
        "should say what to do instead: {msg}"
    );
}
