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
    assert!(INDEX.contains("@get('/dashboard/stream?token=__TOKEN__')"));
    assert!(INDEX.contains(r#"id="queues""#));
    assert!(INDEX.contains(r#"id="workers""#));
    assert!(INDEX.contains(r#"id="health""#));
}

#[test]
fn the_token_placeholder_is_always_substituted() {
    // If the placeholder survived into the served page, the stream URL would
    // carry the literal string and every dashboard would silently fail to
    // authenticate its own SSE connection.
    let rendered = INDEX.replace("__TOKEN__", &encode_attr("abc"));
    assert!(!rendered.contains("__TOKEN__"));
    assert!(rendered.contains("token=abc"));
}

#[test]
fn a_token_cannot_break_out_of_the_attribute() {
    // The token is attacker-chosen -- they control the query string -- and it
    // is echoed into an HTML attribute, which makes it an XSS sink unless the
    // quote characters are escaped.
    let nasty = encode_attr(r#"'><script>alert(1)</script>"#);
    assert!(!nasty.contains('\''), "single quote must be escaped: {nasty}");
    assert!(!nasty.contains('<'), "angle bracket must be escaped: {nasty}");
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
