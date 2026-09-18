//! API request-shape tests.
//!
//! Routing and wire behaviour are covered by the integration test, which runs a
//! real server against a real database.

use super::*;

#[test]
fn enqueue_request_requires_only_queue_and_task() {
    let r: EnqueueRequest = serde_json::from_str(r#"{"queue":"mail","task":"send"}"#).unwrap();
    assert_eq!(r.queue, "mail");
    assert_eq!(r.task, "send");
    assert!(r.unique.is_none());
    assert!(r.tenant_id.is_none());
}

#[test]
fn enqueue_request_accepts_full_shape() {
    let r: EnqueueRequest = serde_json::from_str(
        r#"{"queue":"mail","task":"send","args":{"to":"x"},
            "tenant_id":"acme","priority":5,"max_attempts":3,"unique":"k"}"#,
    )
    .unwrap();
    assert_eq!(r.priority, Some(5));
    assert_eq!(r.max_attempts, Some(3));
    assert_eq!(r.unique.as_deref(), Some("k"));
    assert_eq!(r.args["to"], "x");
}

#[test]
fn deduplicated_enqueue_is_not_an_error() {
    // created=false means "already queued". The caller's intent -- ensure this
    // job exists -- was satisfied, so this is 201 with created:false rather
    // than a 409. Making it an error would push every caller into
    // distinguishing "already queued" from "failed", which is the ergonomic
    // problem the ON CONFLICT DO NOTHING design avoids.
    let r = EnqueueResponse {
        id: None,
        created: false,
    };
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["created"], false);
    assert!(v["id"].is_null());
}
