//! Compiles tests/c/lifecycle.c as C23, runs it on a private schema, and checks the rows it left.
//! The compiler is `$STAMPEDED_CC`, else `gcc-16` if on PATH, else `cc`.

use sqlx::{AssertSqlSafe, PgPool};
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

fn compiler() -> String {
    if let Ok(cc) = std::env::var("STAMPEDED_CC") {
        return cc;
    }
    let found = |c: &str| {
        Command::new(c)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    };
    if found("gcc-16") {
        "gcc-16".into()
    } else {
        "cc".into()
    }
}

/// `deps/`, where `cargo test` put the library; the copy one level up can be stale.
fn library_dir() -> PathBuf {
    let exe = std::env::current_exe().expect("test executable");
    exe.parent().expect("deps dir").to_path_buf()
}

#[tokio::test]
async fn a_c_program_drives_the_whole_lifecycle() {
    let Ok(url) = std::env::var("STAMPEDED_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "STAMPEDED_TEST_DATABASE_URL is not set and CI is"
        );
        eprintln!("skipping: STAMPEDED_TEST_DATABASE_URL not set");
        return;
    };

    // A private schema, reached through the URL's options as a real caller would.
    let schema = format!("c_{}", Uuid::now_v7().simple());
    let admin = PgPool::connect(&url).await.expect("connect");
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("schema");
    sqlx::query(AssertSqlSafe(format!("SET search_path TO {schema}")))
        .execute(&admin)
        .await
        .expect("path");
    let separator = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{separator}options=-c%20search_path%3D{schema}");
    let pool = PgPool::connect(&scoped).await.expect("connect scoped");
    stampeded::schema::migrate(&pool).await.expect("migrate");

    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let libs = library_dir();
    let out = std::env::temp_dir().join(format!("stampeded-lifecycle-{schema}"));
    let cc = compiler();
    let compiled = Command::new(&cc)
        .args(["-std=c23", "-Wall", "-Wextra", "-pedantic", "-Werror"])
        .arg("-I")
        .arg(crate_dir.join("include"))
        .arg(crate_dir.join("tests/c/lifecycle.c"))
        .arg("-L")
        .arg(&libs)
        .arg("-lstampeded_ffi")
        .arg(format!("-Wl,-rpath,{}", libs.display()))
        .arg("-o")
        .arg(&out)
        .output()
        .expect("run the C compiler");
    assert!(
        compiled.status.success(),
        "{cc} failed:\n{}",
        String::from_utf8_lossy(&compiled.stderr)
    );

    let run = Command::new(&out)
        .arg(&scoped)
        .output()
        .expect("run lifecycle");
    let _ = std::fs::remove_file(&out);
    assert!(
        run.status.success(),
        "lifecycle failed ({}):\n{}\n{}",
        run.status,
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );

    let rows: Vec<(String, String, i16, Option<String>, i32)> = sqlx::query_as(
        "SELECT task, state, interruptions, failure_kind, checkpoint_seq FROM stampeded_jobs ORDER BY task",
    )
    .fetch_all(&pool)
    .await
    .expect("rows");
    let row = |task: &str| {
        rows.iter()
            .find(|r| r.0 == task)
            .cloned()
            .unwrap_or_else(|| panic!("no {task} row"))
    };

    assert_eq!(row("greet").1, "completed");
    assert_eq!(row("greet").4, 1, "the checkpoint landed");
    assert_eq!(row("flaky").1, "retryable");
    assert_eq!(row("doomed").1, "discarded");
    assert_eq!(row("doomed").3.as_deref(), Some("unrecoverable"));
    assert_eq!(row("slow").1, "retryable");
    assert_eq!(row("slow").3.as_deref(), Some("timed_out"));
    let abandoned = row("abandoned");
    assert_eq!(
        (abandoned.1.as_str(), abandoned.2),
        ("completed", 1),
        "abandoned once, then completed"
    );

    let workers: i64 = sqlx::query_scalar("SELECT count(*) FROM stampeded_workers")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(workers, 0, "stop deregistered the puller");
}
