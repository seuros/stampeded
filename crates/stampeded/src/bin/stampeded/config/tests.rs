//! Config parsing and validation.

use super::*;

fn no_env(_: &str) -> Option<String> {
    None
}

fn args(s: &[&str]) -> Vec<String> {
    s.iter().map(ToString::to_string).collect()
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
    assert_eq!(c.queues, stampeded::Queues::one("mail"));
}

#[test]
fn dashboard_token_comes_from_the_flag_or_env_and_the_flag_wins() {
    let flag: &[&str] = &["--dashboard-token", "from-flag"];
    let cases: [(&[&str], Option<&str>, Option<&str>); 5] = [
        (&[], None, None),
        (&[], Some("from-env"), Some("from-env")),
        (&[], Some(""), None),
        (flag, None, Some("from-flag")),
        (flag, Some("from-env"), Some("from-flag")),
    ];
    for (extra, env_token, expected) in cases {
        let c = Config::parse(args(extra), |k| match k {
            "STAMPEDED_DATABASE_URL" => Some("postgres://x/y".into()),
            "STAMPEDED_DASHBOARD_TOKEN" => env_token.map(Into::into),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            c.dashboard_token.as_deref(),
            expected,
            "{extra:?} with STAMPEDED_DASHBOARD_TOKEN={env_token:?}"
        );
    }
}

#[test]
fn an_empty_dashboard_token_is_refused() {
    // An empty token matches an empty `?token=`: the dashboard would be open.
    let err = Config::parse(
        args(&["--database-url", "postgres://x/y", "--dashboard-token", ""]),
        no_env,
    )
    .unwrap_err();
    assert!(err.contains("--dashboard-token"), "got: {err}");
}

#[test]
fn a_worker_can_serve_several_queues_or_every_queue() {
    let parse = |extra: &[&str]| {
        Config::parse(args(extra), |k| {
            (k == "STAMPEDED_DATABASE_URL").then(|| "postgres://x/y".into())
        })
    };

    assert_eq!(
        parse(&["--queue", "mail,reports"]).unwrap().queues,
        stampeded::Queues::parse("mail,reports").unwrap()
    );
    let all = parse(&["--queue", "*", "--except-queues", "dlq"])
        .unwrap()
        .queues;
    assert!(all.serves("mail") && !all.serves("dlq"));

    assert!(parse(&["--queue", "mail", "--except-queues", "dlq"]).is_err());
    assert!(parse(&["--queue", "mail,,reports"]).is_err());
}

#[test]
fn rejects_dead_after_that_would_reap_healthy_workers() {
    // With a 30s heartbeat, a 60s dead_after reclaims jobs from paused workers.
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
    // A default >= concurrency would mean connection pinning is back.
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
    let out = super::super::redact("postgres://app:hunter2@db.internal:5432/prod");

    assert!(!out.contains("hunter2"), "password leaked: {out}");
    assert!(out.contains("app"), "the user is useful and not secret");
    assert!(out.contains("db.internal"));
    assert!(out.contains("/prod"));
}

#[test]
fn redaction_survives_urls_with_no_credentials() {
    assert_eq!(
        super::super::redact("postgres://localhost/stampeded"),
        "postgres://localhost/stampeded"
    );
    assert_eq!(super::super::redact("not a url"), "not a url");
}

#[test]
fn a_connect_failure_names_its_fix() {
    // Error shapes as sqlx returns them.
    let cases = [
        (
            sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid peer certificate: UnknownIssuer",
            )),
            "sslrootcert",
        ),
        (
            sqlx::Error::Tls("server does not support TLS".into()),
            "sslmode",
        ),
        (
            sqlx::Error::Io(std::io::ErrorKind::ConnectionRefused.into()),
            "Nothing is listening",
        ),
    ];
    for (error, hint) in cases {
        let out = super::super::explain_connect_failure(&error, "postgres://db/app").to_string();
        assert!(out.contains(hint), "{error}: expected {hint:?} in {out:?}");
    }
}

#[test]
fn a_fatal_error_prints_its_newlines() {
    // `main` prints its error with Debug; anyhow's Debug must keep real newlines.
    let f = anyhow::anyhow!("first line\nsecond line");

    assert!(format!("{f:?}").starts_with("first line\nsecond line"));
    assert!(
        !format!("{f:?}").contains("\\n"),
        "escapes leaked into the output"
    );
}
