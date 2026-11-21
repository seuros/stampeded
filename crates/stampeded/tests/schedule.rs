//! Schedule job templates, exercised against isolated PostgreSQL schemas.

mod common;

use serde_json::json;
use sqlx::AssertSqlSafe;
use stampeded::{
    Error, FailureKind, Job, JobState, Queue, ScheduleChange, ScheduleSpec, SyncChange,
};
use std::time::Duration;

async fn bounded(q: &Queue, key: &str) {
    sqlx::query(
        "INSERT INTO stampeded_schedules
             (key, queue, task, interval_secs, next_run_at,
              max_attempts, max_interruptions, expire_seconds)
         VALUES ($1, 'default', 'report', 60, now() - interval '1 minute', 3, 0, 1)",
    )
    .bind(key)
    .execute(q.pool())
    .await
    .expect("bounded schedule");
}

async fn only_job(q: &Queue) -> Job {
    let mut jobs: Vec<Job> = sqlx::query_as("SELECT * FROM stampeded_jobs")
        .fetch_all(q.pool())
        .await
        .expect("scheduled jobs");
    assert_eq!(jobs.len(), 1, "exactly one job, including after replay");
    jobs.remove(0)
}

#[tokio::test]
async fn old_inserts_and_registration_calls_keep_the_old_defaults() {
    let q = q!();
    sqlx::query(
        "INSERT INTO stampeded_schedules (key, queue, task, interval_secs)
         VALUES ('sql', 'default', 'report', 60)",
    )
    .execute(q.pool())
    .await
    .expect("old SQL insert");
    q.upsert_schedule("rust", "default", "report", Duration::from_secs(60))
        .await
        .expect("old Rust registration");
    assert_eq!(q.run_due_schedules(10).await.expect("fire"), 2);
    let limits: Vec<(i16, i16, i32)> = sqlx::query_as(
        "SELECT max_attempts, max_interruptions, expire_seconds FROM stampeded_jobs",
    )
    .fetch_all(q.pool())
    .await
    .expect("old defaults");
    assert_eq!(limits, vec![(25, 1, 3600); 2]);
}

#[tokio::test]
async fn interval_and_cron_occurrences_copy_the_template_and_keep_existing_jobs() {
    let q = q!();
    bounded(&q, "interval").await;
    bounded(&q, "cron").await;
    sqlx::query(
        "UPDATE stampeded_schedules SET
             args = '{\"account\":42}', meta = '{\"runtime\":\"rust\"}',
             tenant_id = 'acme', priority = 7",
    )
    .execute(q.pool())
    .await
    .expect("payload");
    sqlx::query(
        "UPDATE stampeded_schedules SET interval_secs = NULL, cron_expr = '* * * * *',
             dst_policy = 'wall_clock', timezone = 'Mars/Olympus_Mons'
         WHERE key = 'cron'",
    )
    .execute(q.pool())
    .await
    .expect("cron with UTC fallback");
    assert_eq!(q.run_due_schedules(10).await.expect("both rules fire"), 2);
    let jobs: Vec<Job> = sqlx::query_as("SELECT * FROM stampeded_jobs ORDER BY id")
        .fetch_all(q.pool())
        .await
        .expect("jobs");
    for job in &jobs {
        assert_eq!(
            (job.max_attempts, job.max_interruptions, job.expire_seconds),
            (3, 0, 1)
        );
        assert_eq!(job.args, json!({"account": 42}));
        assert_eq!(job.meta, json!({"runtime": "rust"}));
        assert_eq!(job.tenant_id.as_deref(), Some("acme"));
        assert_eq!(job.priority, 7);
    }

    sqlx::query(
        "UPDATE stampeded_schedules SET max_attempts = 5, max_interruptions = 2,
             expire_seconds = 90, next_run_at = now() - interval '2 minutes'",
    )
    .execute(q.pool())
    .await
    .expect("edit future jobs");
    // Updating the rule through the old helper must not reset template options.
    q.upsert_schedule("interval", "default", "report", Duration::from_secs(120))
        .await
        .expect("update interval");
    q.upsert_cron_schedule("cron", "default", "report", "* * * * *", "UTC", None)
        .await
        .expect("update cron");
    sqlx::query(
        "UPDATE stampeded_schedules SET next_run_at = now() - interval '2 minutes'
         WHERE key = 'cron'",
    )
    .execute(q.pool())
    .await
    .expect("make cron due");
    assert_eq!(q.run_due_schedules(10).await.expect("next occurrences"), 2);
    for old in jobs {
        let unchanged = q
            .fetch(&old.queue, old.id)
            .await
            .expect("fetch")
            .expect("job");
        assert_eq!(
            (
                unchanged.max_attempts,
                unchanged.max_interruptions,
                unchanged.expire_seconds
            ),
            (3, 0, 1)
        );
    }
    let newer: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM stampeded_jobs
         WHERE max_attempts = 5 AND max_interruptions = 2 AND expire_seconds = 90",
    )
    .fetch_one(q.pool())
    .await
    .expect("new limits");
    assert_eq!(newer, 2);
}

#[tokio::test]
async fn a_scheduled_three_attempt_budget_really_stops_after_three_failures() {
    let q = q!();
    bounded(&q, "retry").await;
    assert_eq!(q.run_due_schedules(10).await.expect("fire"), 1);
    let worker = q
        .register_worker("test", 1, &["default".into()])
        .await
        .expect("worker");
    for attempt in 1..=3 {
        let mut claimed = q.claim("default", worker, 1).await.expect("claim");
        assert_eq!(claimed.len(), 1);
        let job = claimed.remove(0);
        assert_eq!(job.attempt, attempt);
        q.fail(
            &job,
            worker,
            FailureKind::Raised,
            "failed",
            Some(Duration::ZERO),
        )
        .await
        .expect("fail");
        let expected = if attempt == 3 {
            JobState::Discarded
        } else {
            JobState::Retryable
        };
        assert_eq!(only_job(&q).await.state, expected);
    }
    assert!(
        q.claim("default", worker, 1)
            .await
            .expect("exhausted")
            .is_empty()
    );
}

#[tokio::test]
async fn a_scheduled_zero_interruption_budget_is_terminal_on_the_first_worker_death() {
    let q = q!();
    bounded(&q, "interrupt").await;
    q.run_due_schedules(10).await.expect("fire");
    let worker = q
        .register_worker("test", 1, &["default".into()])
        .await
        .expect("worker");
    let job = q
        .claim("default", worker, 1)
        .await
        .expect("claim")
        .remove(0);
    q.fail(&job, worker, FailureKind::WorkerExited, "worker died", None)
        .await
        .expect("interruption");
    let failed = only_job(&q).await;
    assert_eq!(failed.state, JobState::Discarded);
    assert_eq!(failed.interruptions, 1);
    assert_eq!(failed.attempt, 1);
}

#[tokio::test]
async fn scheduled_expiry_uses_the_configured_inactivity_seconds() {
    let q = q!();
    bounded(&q, "expiry").await;
    q.run_due_schedules(10).await.expect("fire");
    let worker = q
        .register_worker("test", 1, &["default".into()])
        .await
        .expect("worker");
    q.claim("default", worker, 1).await.expect("claim");
    sqlx::query("UPDATE stampeded_jobs SET heartbeat_at = now() - interval '2 seconds'")
        .execute(q.pool())
        .await
        .expect("simulate inactivity without sleeping");
    assert_eq!(q.expire(10).await.expect("expire"), 1);
    let expired = only_job(&q).await;
    assert_eq!(expired.state, JobState::Retryable);
    assert_eq!(expired.failure_kind, Some(FailureKind::TimedOut));
    assert_eq!(expired.attempt, 1);
}

#[tokio::test]
async fn concurrent_ticks_and_replayed_occurrences_do_not_duplicate_or_rewrite_jobs() {
    let q = q!();
    bounded(&q, "once").await;
    let original: jiff_sqlx::Timestamp =
        sqlx::query_scalar("SELECT next_run_at FROM stampeded_schedules WHERE key = 'once'")
            .fetch_one(q.pool())
            .await
            .expect("occurrence instant");
    let (a, b) = tokio::join!(q.run_due_schedules(10), q.run_due_schedules(10));
    assert_eq!(a.expect("first tick") + b.expect("second tick"), 1);
    let job = only_job(&q).await;
    sqlx::query(
        "UPDATE stampeded_schedules SET next_run_at = $1,
             max_attempts = 8, max_interruptions = 4, expire_seconds = 300",
    )
    .bind(original)
    .execute(q.pool())
    .await
    .expect("replay occurrence with edited template");
    assert_eq!(q.run_due_schedules(10).await.expect("replay"), 0);
    let unchanged = only_job(&q).await;
    assert_eq!(unchanged.id, job.id);
    assert_eq!(
        (
            unchanged.max_attempts,
            unchanged.max_interruptions,
            unchanged.expire_seconds
        ),
        (3, 0, 1)
    );
    let ledger: (i64, bool) =
        sqlx::query_as("SELECT count(*), bool_and(job_id = $1) FROM stampeded_cron_occurrences")
            .bind(job.id)
            .fetch_one(q.pool())
            .await
            .expect("one occurrence");
    assert_eq!(ledger, (1, true));
    let advanced: bool = sqlx::query_scalar("SELECT next_run_at > now() FROM stampeded_schedules")
        .fetch_one(q.pool())
        .await
        .expect("advanced despite duplicate");
    assert!(advanced);
}

#[tokio::test]
async fn invalid_schedule_limits_fail_at_insert_and_update_not_during_a_tick() {
    let q = q!();
    bounded(&q, "valid").await;
    for (column, invalid, code) in [
        ("max_attempts", Some(0_i64), "23514"),
        ("max_attempts", Some(-1), "23514"),
        ("max_attempts", None, "23502"),
        ("max_attempts", Some(32768), "22003"),
        ("max_interruptions", Some(-1), "23514"),
        ("max_interruptions", None, "23502"),
        ("max_interruptions", Some(32768), "22003"),
        ("expire_seconds", Some(0), "23514"),
        ("expire_seconds", Some(-1), "23514"),
        ("expire_seconds", None, "23502"),
        ("expire_seconds", Some(2_147_483_648), "22003"),
    ] {
        for sql in [
            format!(
                "INSERT INTO stampeded_schedules (key, queue, task, interval_secs, {column})
                 VALUES ('invalid', 'default', 'report', 60, $1::bigint)"
            ),
            format!("UPDATE stampeded_schedules SET {column} = $1::bigint WHERE key = 'valid'"),
        ] {
            let err = sqlx::query(AssertSqlSafe(sql))
                .bind(invalid)
                .execute(q.pool())
                .await
                .expect_err("reject invalid setting");
            let db = err.as_database_error().expect("database error");
            assert_eq!(
                db.code().as_deref(),
                Some(code),
                "{column}={invalid:?}: {err}"
            );
            if code == "23514" {
                assert_eq!(
                    db.constraint(),
                    Some(format!("stampeded_schedules_{column}_check").as_str())
                );
            }
        }
    }
    // Both ends of every supported range are valid, including no free interruptions.
    for (attempts, interruptions, seconds) in
        [(1_i16, 0_i16, 1_i32), (i16::MAX, i16::MAX, i32::MAX)]
    {
        sqlx::query(
            "UPDATE stampeded_schedules SET max_attempts = $1,
                 max_interruptions = $2, expire_seconds = $3 WHERE key = 'valid'",
        )
        .bind(attempts)
        .bind(interruptions)
        .bind(seconds)
        .execute(q.pool())
        .await
        .expect("valid boundaries");
    }
    assert_eq!(
        q.run_due_schedules(10)
            .await
            .expect("valid schedule still fires"),
        1
    );
}

fn every(key: &str) -> ScheduleSpec {
    ScheduleSpec::every(key, "default", "report", Duration::from_secs(60))
}

fn changes(list: &[(&str, SyncChange)]) -> Vec<ScheduleChange> {
    list.iter()
        .map(|(key, change)| ScheduleChange {
            key: (*key).to_owned(),
            change: *change,
        })
        .collect()
}

/// Every schedule as `(key, owner)`, by key.
async fn owners(q: &Queue) -> Vec<(String, Option<String>)> {
    sqlx::query_as("SELECT key, owner FROM stampeded_schedules ORDER BY key COLLATE \"C\"")
        .fetch_all(q.pool())
        .await
        .expect("owners")
}

fn owned(rows: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
    rows.iter()
        .map(|(k, o)| ((*k).to_owned(), o.map(str::to_owned)))
        .collect()
}

#[tokio::test]
async fn a_sync_upserts_and_prunes_only_its_own_rows() {
    let q = q!();
    q.sync_schedules("worker", &[every("w.ping")])
        .await
        .expect("other owner");
    q.upsert_schedule("bare", "default", "report", Duration::from_secs(60))
        .await
        .expect("unowned row");

    let first = q
        .sync_schedules("web", &[every("a"), every("b")])
        .await
        .expect("first sync");
    assert_eq!(
        first,
        changes(&[("a", SyncChange::Created), ("b", SyncChange::Created)])
    );

    let again = q
        .sync_schedules("web", &[every("a"), every("b")])
        .await
        .expect("idempotent");
    assert!(
        again.is_empty(),
        "an unchanged sync writes nothing: {again:?}"
    );

    let pruned = q
        .sync_schedules(
            "web",
            &[ScheduleSpec::every(
                "a",
                "default",
                "report",
                Duration::from_secs(120),
            )],
        )
        .await
        .expect("prune");
    assert_eq!(
        pruned,
        changes(&[("a", SyncChange::Updated), ("b", SyncChange::Removed)])
    );
    assert_eq!(
        owners(&q).await,
        owned(&[
            ("a", Some("web")),
            ("bare", None),
            ("w.ping", Some("worker"))
        ])
    );

    let emptied = q.sync_schedules("web", &[]).await.expect("empty list");
    assert_eq!(emptied, changes(&[("a", SyncChange::Removed)]));
    assert_eq!(
        owners(&q).await,
        owned(&[("bare", None), ("w.ping", Some("worker"))])
    );
}

#[tokio::test]
async fn an_unowned_row_with_a_listed_key_is_adopted_keeping_its_state() {
    let q = q!();
    q.upsert_schedule("legacy", "default", "report", Duration::from_secs(60))
        .await
        .expect("legacy row");
    sqlx::query(
        "UPDATE stampeded_schedules SET paused = true, max_attempts = 7,
             next_run_at = now() + interval '1 hour' WHERE key = 'legacy'",
    )
    .execute(q.pool())
    .await
    .expect("operator state");
    let before: jiff_sqlx::Timestamp =
        sqlx::query_scalar("SELECT next_run_at FROM stampeded_schedules")
            .fetch_one(q.pool())
            .await
            .expect("next run");

    let adopted = q
        .sync_schedules("web", &[every("legacy")])
        .await
        .expect("adopt");
    assert_eq!(adopted, changes(&[("legacy", SyncChange::Adopted)]));
    let (owner, paused, attempts, next): (Option<String>, bool, i16, jiff_sqlx::Timestamp) =
        sqlx::query_as("SELECT owner, paused, max_attempts, next_run_at FROM stampeded_schedules")
            .fetch_one(q.pool())
            .await
            .expect("adopted row");
    assert_eq!(owner.as_deref(), Some("web"));
    assert!(paused, "a sync never unpauses");
    assert_eq!(attempts, 7, "an omitted limit keeps the row's");
    assert_eq!(next, before, "the same rule keeps its next run");

    // The older helpers still work on an owned row, and do not disown it.
    q.upsert_schedule("legacy", "default", "report", Duration::from_secs(30))
        .await
        .expect("old helper");
    assert_eq!(owners(&q).await, owned(&[("legacy", Some("web"))]));
}

#[tokio::test]
async fn a_key_owned_by_another_program_is_refused_and_nothing_is_written() {
    let q = q!();
    q.sync_schedules("worker", &[every("shared"), every("w.only")])
        .await
        .expect("worker's schedules");
    q.sync_schedules("web", &[every("web.old")])
        .await
        .expect("web's schedules");

    let err = q
        .sync_schedules("web", &[every("aaa.new"), every("shared")])
        .await
        .expect_err("another owner's key");
    let Error::InvalidSchedule(message) = &err else {
        panic!("expected InvalidSchedule, got {err:?}");
    };
    assert!(
        message.contains("'shared'") && message.contains("'worker'"),
        "{message}"
    );
    // The whole sync rolled back: no new row, and the unlisted row survives.
    assert_eq!(
        owners(&q).await,
        owned(&[
            ("shared", Some("worker")),
            ("w.only", Some("worker")),
            ("web.old", Some("web"))
        ])
    );
}

#[tokio::test]
async fn sync_limits_are_set_when_given_and_preserved_when_omitted() {
    let q = q!();
    q.sync_schedules(
        "web",
        &[
            every("tuned")
                .max_attempts(3)
                .max_interruptions(0)
                .expires_in(Duration::from_secs(120)),
            every("plain"),
        ],
    )
    .await
    .expect("create with limits");
    let limits = || async {
        sqlx::query_as::<_, (String, i16, i16, i32)>(
            "SELECT key, max_attempts, max_interruptions, expire_seconds
               FROM stampeded_schedules ORDER BY key",
        )
        .fetch_all(q.pool())
        .await
        .expect("limits")
    };
    assert_eq!(
        limits().await,
        vec![
            ("plain".to_owned(), 25, 1, 3600),
            ("tuned".to_owned(), 3, 0, 120)
        ]
    );

    sqlx::query("UPDATE stampeded_schedules SET max_attempts = 9 WHERE key = 'plain'")
        .execute(q.pool())
        .await
        .expect("tuned in SQL");
    let resync = q
        .sync_schedules("web", &[every("tuned"), every("plain")])
        .await
        .expect("resync without limits");
    assert!(
        resync.is_empty(),
        "omitted limits are not a change: {resync:?}"
    );
    assert_eq!(
        limits().await,
        vec![
            ("plain".to_owned(), 9, 1, 3600),
            ("tuned".to_owned(), 3, 0, 120)
        ]
    );

    let changed = q
        .sync_schedules("web", &[every("tuned").max_attempts(5), every("plain")])
        .await
        .expect("new limit");
    assert_eq!(changed, changes(&[("tuned", SyncChange::Updated)]));
    sqlx::query("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'")
        .execute(q.pool())
        .await
        .expect("make due");
    assert_eq!(q.run_due_schedules(10).await.expect("fire"), 2);
    let fired: Vec<(i16, i16, i32)> = sqlx::query_as(
        "SELECT max_attempts, max_interruptions, expire_seconds FROM stampeded_jobs
          ORDER BY max_attempts",
    )
    .fetch_all(q.pool())
    .await
    .expect("jobs");
    assert_eq!(fired, vec![(5, 0, 120), (9, 1, 3600)]);
}

#[tokio::test]
async fn a_synced_cron_is_seeded_and_moves_only_when_its_rule_does() {
    let q = q!();
    let nightly = |expr: &str| {
        ScheduleSpec::cron("nightly", "default", "report", expr)
            .timezone("Africa/Casablanca")
            .args(json!({"account": 42}))
            .priority(3)
    };
    q.sync_schedules("web", &[nightly("0 3 * * *")])
        .await
        .expect("create");
    let row = || async {
        sqlx::query_as::<_, (jiff_sqlx::Timestamp, String, String, serde_json::Value, i16)>(
            "SELECT next_run_at, dst_policy, timezone, args, priority FROM stampeded_schedules",
        )
        .fetch_one(q.pool())
        .await
        .expect("cron row")
    };
    let (seeded, policy, zone, args, priority) = row().await;
    assert!(seeded.to_jiff() > common::db_now(&q).await, "seeded ahead");
    assert_eq!(policy, "wall_clock", "a named hour suggests wall_clock");
    assert_eq!(zone, "Africa/Casablanca");
    assert_eq!(args, json!({"account": 42}));
    assert_eq!(priority, 3);
    assert_eq!(q.run_due_schedules(10).await.expect("tick"), 0);

    q.sync_schedules("web", &[nightly("0 3 * * *")])
        .await
        .expect("same rule");
    assert_eq!(row().await.0, seeded);

    q.sync_schedules("web", &[nightly("*/5 * * * *")])
        .await
        .expect("new rule");
    let (moved, policy, ..) = row().await;
    assert_ne!(moved, seeded);
    assert_eq!(policy, "instant");
}

#[tokio::test]
async fn malformed_syncs_are_refused_before_anything_is_written() {
    let q = q!();
    for (owner, specs) in [
        ("  ", vec![every("a")]),
        ("web", vec![every("a"), every("a")]),
        ("web", vec![every(" ")]),
        (
            "web",
            vec![ScheduleSpec::every(
                "z",
                "default",
                "report",
                Duration::ZERO,
            )],
        ),
        (
            "web",
            vec![ScheduleSpec::cron("c", "default", "report", "nonsense")],
        ),
        (
            "web",
            vec![ScheduleSpec::cron("c", "default", "report", "* * * * *").timezone("Mars/Base")],
        ),
        ("web", vec![every("a").args(json!([1, 2]))]),
    ] {
        let err = q
            .sync_schedules(owner, &specs)
            .await
            .expect_err("malformed sync");
        assert!(
            matches!(err, Error::InvalidSchedule(_)),
            "{owner:?} {specs:?}: {err:?}"
        );
    }
    assert_eq!(owners(&q).await, owned(&[]));
}
