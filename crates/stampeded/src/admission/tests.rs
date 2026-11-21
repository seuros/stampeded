use super::paused_note;
use serde_json::json;

#[test]
fn a_listing_names_why_a_worker_claims_nothing() {
    let paused = json!({ "admitting": false, "reason": "disk:/data warning: 1.0 GiB free" });
    assert_eq!(
        paused_note(Some(&paused)).as_deref(),
        Some("claiming paused: disk:/data warning: 1.0 GiB free")
    );
    let unexplained = json!({ "admitting": false });
    assert_eq!(
        paused_note(Some(&unexplained)).as_deref(),
        Some("claiming paused: host vitals")
    );
}

#[test]
fn a_claiming_worker_or_one_without_vitals_gets_no_note() {
    assert_eq!(paused_note(None), None);
    assert_eq!(paused_note(Some(&json!({ "admitting": true }))), None);
    assert_eq!(paused_note(Some(&json!("garbage"))), None);
}
