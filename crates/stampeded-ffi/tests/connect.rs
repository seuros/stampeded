//! `stampeded_connect` on a bad URL fails at once, and `stampeded_last_error` says why.
//! The role and database cases need STAMPEDED_TEST_DATABASE_URL.

use sqlx::ConnectOptions;
use sqlx::postgres::PgConnectOptions;
use stampeded_ffi::stampeded_last_error;
use std::ffi::{CStr, CString, c_char, c_void};
use std::time::{Duration, Instant};

// By their C names: the module that defines them is private, so they have no Rust path.
#[allow(unsafe_code, reason = "declares the C ABI")]
unsafe extern "C" {
    fn stampeded_connect(database_url: *const c_char) -> *mut c_void;
    fn stampeded_client_free(client: *mut c_void);
}

/// Far below the pool's 30 s, far above a refused connect or a failed lookup.
const PROMPTLY: Duration = Duration::from_secs(5);

/// A loopback port nothing listens on: bound, then released.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("local addr").port()
}

/// `STAMPEDED_TEST_DATABASE_URL`, or `None` to skip -- except under CI.
fn database_url() -> Option<String> {
    let url = std::env::var("STAMPEDED_TEST_DATABASE_URL").ok();
    if url.is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "STAMPEDED_TEST_DATABASE_URL is not set and CI is; refusing to skip"
        );
        eprintln!("skipping the database cases: STAMPEDED_TEST_DATABASE_URL not set");
    }
    url
}

/// `stampeded_connect(url)`: how long it took, and the error it left, if any.
#[allow(unsafe_code, reason = "calls the C ABI")]
fn connect(url: &str) -> (Duration, Option<String>) {
    let url = CString::new(url).expect("url has no NUL");
    let started = Instant::now();
    // SAFETY: url is NUL-terminated and outlives the call.
    let client = unsafe { stampeded_connect(url.as_ptr()) };
    let took = started.elapsed();
    if !client.is_null() {
        // SAFETY: client came from stampeded_connect and is not used again.
        unsafe { stampeded_client_free(client) };
        return (took, None);
    }
    let error = stampeded_last_error();
    assert!(!error.is_null(), "NULL client with no error after {took:?}");
    // SAFETY: non-NULL, so NUL-terminated and valid until this thread's next call.
    let error = unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned();
    (took, Some(error))
}

#[test]
fn a_connect_that_cannot_work_fails_promptly_and_says_why() {
    // (url, a word the error must contain)
    let mut cases = vec![
        (
            format!("postgres://nobody@127.0.0.1:{}/none", closed_port()),
            "refused".to_string(),
        ),
        (
            "postgres://nobody@stampeded-no-such-host.invalid/none".into(),
            "lookup".into(),
        ),
    ];
    if let Some(url) = database_url() {
        let good: PgConnectOptions = url.parse().expect("STAMPEDED_TEST_DATABASE_URL");
        let (took, error) = connect(&url);
        assert_eq!(error, None, "the test database itself, after {took:?}");

        // Trust auth reports an unknown role, password auth a failed password: both name the user.
        let role = "stampeded_no_such_role";
        cases.push((
            good.clone().username(role).to_url_lossy().into(),
            role.into(),
        ));
        let database = "stampeded_no_such_database";
        cases.push((
            good.database(database).to_url_lossy().into(),
            database.into(),
        ));
    }

    let mut wrong = Vec::new();
    for (url, why) in &cases {
        let (took, error) = connect(url);
        let error = error.unwrap_or_default();
        eprintln!("{url}: {took:?}: {error}");
        if took >= PROMPTLY || !error.contains(why.as_str()) {
            wrong.push(format!("{url}: {took:?}, expected {why:?} in {error:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
