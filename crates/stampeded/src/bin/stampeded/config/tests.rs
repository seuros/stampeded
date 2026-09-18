//! Config parsing and validation.

use super::*;

fn no_env(_: &str) -> Option<String> {
    None
}

fn args(s: &[&str]) -> Vec<String> {
    s.iter().map(|x| x.to_string()).collect()
}

#[test]
fn database_url_is_required() {
    let err = Config::parse(vec![], no_env).unwrap_err();
    assert!(err.contains("database-url"), "got: {err}");
}

#[test]
fn database_url_can_come_from_env() {
    let c = Config::parse(vec![], |k| {
        (k == "STAMPEDED_DATABASE_URL").then(|| "postgres://x/y".to_string())
    })
    .unwrap();
    assert_eq!(c.database_url, "postgres://x/y");
}

#[test]
fn flags_override_env() {
    let c = Config::parse(args(&["--queue", "mail"]), |k| match k {
        "STAMPEDED_DATABASE_URL" => Some("postgres://x/y".into()),
        "STAMPEDED_QUEUE" => Some("from-env".into()),
        _ => None,
    })
    .unwrap();
    assert_eq!(c.queue, "mail");
}

#[test]
fn rejects_dead_after_that_would_reap_healthy_workers() {
    // The invariant worth failing startup over: with a 30s heartbeat, a 60s
    // dead_after reclaims jobs from workers that merely paused. That presents
    // as random duplicate execution, which is miserable to diagnose from the
    // outside.
    let err = Config::parse(
        args(&["--database-url", "postgres://x/y", "--dead-after", "60"]),
        no_env,
    )
    .unwrap_err();
    assert!(err.contains("too tight"), "got: {err}");
}

#[test]
fn accepts_dead_after_with_enough_margin() {
    let c = Config::parse(
        args(&["--database-url", "postgres://x/y", "--dead-after", "90"]),
        no_env,
    )
    .unwrap();
    assert_eq!(c.dead_after.as_secs(), 90);
}

#[test]
fn rejects_zero_concurrency() {
    let err = Config::parse(
        args(&["--database-url", "postgres://x/y", "--concurrency", "0"]),
        no_env,
    )
    .unwrap_err();
    assert!(err.contains("at least 1"), "got: {err}");
}

#[test]
fn rejects_unknown_options_rather_than_ignoring_them() {
    // Silently ignoring a typo'd flag means running with defaults the operator
    // did not ask for.
    let err = Config::parse(
        args(&["--database-url", "postgres://x/y", "--concurency", "5"]),
        no_env,
    )
    .unwrap_err();
    assert!(err.contains("unknown option"), "got: {err}");
}

#[test]
fn reports_missing_values_by_name() {
    let err = Config::parse(args(&["--database-url"]), no_env).unwrap_err();
    assert!(
        err.contains("--database-url requires a value"),
        "got: {err}"
    );
}

#[test]
fn pool_size_defaults_below_concurrency() {
    // A connection is held only while claiming or reporting, never for the
    // job's duration -- the underway trap (plans/14 section 5). If this ever
    // defaults to >= concurrency, someone has reintroduced connection pinning.
    let c = Config::parse(args(&["--database-url", "postgres://x/y"]), no_env).unwrap();
    assert!((c.pool_size as usize) < c.concurrency);
}

#[test]
fn roles_can_be_disabled_independently() {
    let c = Config::parse(
        args(&["--database-url", "postgres://x/y", "--no-worker"]),
        no_env,
    )
    .unwrap();
    assert!(!c.worker);
    assert!(c.maintenance);
}

// ---------------------------------------------------------- startup failures

#[test]
fn a_connection_url_never_shows_its_password() {
    // It goes into a log line at startup, and a log line is the easiest secret
    // to leak by accident -- someone pastes a startup error into an issue.
    let out = super::super::redact("postgres://app:hunter2@db.internal:5432/prod");

    assert!(!out.contains("hunter2"), "password leaked: {out}");
    assert!(out.contains("app"), "the user is useful and not secret");
    assert!(out.contains("db.internal"));
    assert!(out.contains("/prod"));
}

#[test]
fn redaction_survives_urls_with_no_credentials() {
    // Local development URLs often have neither, and mangling them would make
    // the log line wrong exactly where it is most read.
    assert_eq!(
        super::super::redact("postgres://localhost/stampeded"),
        "postgres://localhost/stampeded"
    );
    assert_eq!(super::super::redact("not a url"), "not a url");
}

#[test]
fn a_fatal_error_prints_its_newlines() {
    // `main` returning Result prints the error with DEBUG, so a String comes
    // out quoted with literal \n escapes -- which turns a carefully written
    // multi-line hint into one unreadable line. This is the bug that trick
    // exists to avoid, so it gets a test.
    let f = super::super::Fatal("first line\nsecond line".to_string());

    assert_eq!(format!("{f:?}"), "first line\nsecond line");
    assert!(
        !format!("{f:?}").contains("\\n"),
        "escapes leaked into the output"
    );
}
