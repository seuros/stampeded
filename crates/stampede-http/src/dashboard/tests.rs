//! Dashboard rendering tests.

use super::*;

#[test]
fn escapes_html_in_operator_controlled_values() {
    // Queue and host names are operator-controlled, not end-user input, but
    // they still reach a browser. Escaping costs nothing and removes a whole
    // class of question.
    assert_eq!(esc(r#"<script>"#), "&lt;script&gt;");
    assert_eq!(esc(r#"a&b"c"#), "a&amp;b&quot;c");
}

#[test]
fn index_bootstraps_the_stream_and_has_patch_targets() {
    // The page does nothing except open the stream; everything after is a
    // patched fragment. If the ids drift from the ones render_* emit, the
    // dashboard silently stops updating.
    assert!(INDEX.contains("@get('/dashboard/stream')"));
    assert!(INDEX.contains(r#"id="queues""#));
    assert!(INDEX.contains(r#"id="workers""#));
}

#[test]
fn patch_produces_an_sse_event() {
    let ev = patch("<div id=\"queues\">x</div>".to_string());
    assert!(ev.is_some(), "PatchElements should build a valid SSE event");
}

#[test]
fn patch_rejects_empty_elements() {
    // PatchElements requires a non-empty payload; an empty render must not
    // produce a malformed event that breaks the stream.
    assert!(patch(String::new()).is_none());
}
