//! API request-shape tests; routing is covered by the integration test.

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
    // created=false means "already queued", not a failure.
    let r = EnqueueResponse {
        id: None,
        created: false,
    };
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["created"], false);
    assert!(v["id"].is_null());
}

#[tokio::test]
async fn a_poisoned_panel_cache_still_renders() {
    let cache = PanelCache::default();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = cache.inner.lock().unwrap();
        panic!("poison the cache");
    }));
    assert!(poisoned.is_err());
    assert!(cache.inner.is_poisoned());

    let html = cache
        .get("health", Duration::from_secs(60), async {
            Some("x".to_string())
        })
        .await;
    assert_eq!(html.as_deref(), Some("x"));
    let cached = cache
        .get("health", Duration::from_secs(60), async { None })
        .await;
    assert_eq!(
        cached.as_deref(),
        Some("x"),
        "the render was stored despite the poison"
    );
}
