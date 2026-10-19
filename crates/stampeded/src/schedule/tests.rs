//! Next-run arithmetic; the firing itself is covered by the integration tests.

use super::*;

fn at(s: &str) -> Timestamp {
    s.parse().expect("rfc3339")
}

fn interval(secs: i64, next_run_at: &str) -> Schedule {
    Schedule {
        key: "t".into(),
        queue: "default".into(),
        task: "t".into(),
        interval_secs: Some(secs),
        cron_expr: None,
        next_run_at: at(next_run_at),
        dst_policy: "instant".into(),
        timezone: "UTC".into(),
        missed_policy: "once".into(),
    }
}

fn cron(expr: &str, tz: &str, policy: &str, next_run_at: &str) -> Schedule {
    Schedule {
        key: "t".into(),
        queue: "default".into(),
        task: "t".into(),
        interval_secs: None,
        cron_expr: Some(expr.into()),
        next_run_at: at(next_run_at),
        dst_policy: policy.into(),
        timezone: tz.into(),
        missed_policy: "once".into(),
    }
}

#[test]
fn interval_advances_from_the_target_not_from_now() {
    // A late run still lands on the original grid.
    let s = interval(3600, "2026-09-17T10:00:00Z");
    let late = at("2026-09-17T10:00:40Z");
    assert_eq!(s.next_after(late).unwrap(), at("2026-09-17T11:00:00Z"));
}

#[test]
fn a_schedule_far_behind_catches_up_without_backfilling() {
    // Three days of missed hourly runs must not become 72 queued jobs.
    let s = interval(3600, "2026-09-14T10:00:00Z");
    let now = at("2026-09-17T10:30:00Z");
    let next = s.next_after(now).unwrap();
    assert!(next > now, "must land in the future");
    assert_eq!(
        next,
        at("2026-09-17T11:00:00Z"),
        "and stay on the original grid"
    );
}

#[test]
fn realignment_is_bounded_rather_than_unbounded() {
    // ~315M steps would hold the row lock for minutes, so it resumes from now.
    let s = interval(1, "2016-01-01T00:00:00Z");
    let now = at("2026-09-17T10:00:00Z");
    let next = s.next_after(now).unwrap();
    assert!(next > now);
    assert!(
        next <= now + jiff::SignedDuration::from_secs(2),
        "resumed from now, not from 2016"
    );
}

#[test]
fn skip_policy_also_lands_in_the_future() {
    let mut s = interval(3600, "2026-09-14T10:00:00Z");
    s.missed_policy = "skip".into();
    let now = at("2026-09-17T10:30:00Z");
    assert!(s.next_after(now).unwrap() > now);
}

#[test]
fn cron_schedules_use_the_expression() {
    let s = cron("0 3 * * *", "UTC", "instant", "2026-09-17T03:00:00Z");
    assert_eq!(
        s.next_after(at("2026-09-17T03:00:05Z")).unwrap(),
        at("2026-09-18T03:00:00Z")
    );
}

#[test]
fn cron_respects_the_schedules_timezone() {
    // 03:00 in Tokyo is 18:00 UTC the previous day.
    let s = cron(
        "0 3 * * *",
        "Asia/Tokyo",
        "wall_clock",
        "2026-09-17T18:00:00Z",
    );
    assert_eq!(
        s.next_after(at("2026-09-17T18:00:05Z")).unwrap(),
        at("2026-09-18T18:00:00Z")
    );
}

#[test]
fn wall_clock_daily_does_not_repeat_across_the_fold() {
    // 2026-11-01 01:30 New York happens twice; the next run is 25 real hours later, not 1.
    let s = cron(
        "30 1 * * *",
        "America/New_York",
        "wall_clock",
        "2026-11-01T05:30:00Z",
    );
    let next = s.next_after(at("2026-11-01T05:30:05Z")).unwrap();
    assert_eq!(next, at("2026-11-02T06:30:00Z"));
    assert_eq!(
        next.duration_since(at("2026-11-01T05:30:00Z")).as_hours(),
        25
    );
}

#[test]
fn instant_policy_does_repeat_across_the_fold() {
    // The second 01:30, one real hour later.
    let s = cron(
        "30 1 * * *",
        "America/New_York",
        "instant",
        "2026-11-01T05:30:00Z",
    );
    let next = s.next_after(at("2026-11-01T05:30:05Z")).unwrap();
    assert_eq!(next, at("2026-11-01T06:30:00Z"));
}

#[test]
fn a_bad_cron_expression_is_an_error_not_a_panic() {
    let s = cron("not a cron", "UTC", "instant", "2026-09-17T00:00:00Z");
    assert!(s.next_after(at("2026-09-17T00:00:05Z")).is_err());
}

#[test]
fn an_unknown_timezone_falls_back_to_utc_rather_than_failing() {
    // Firing in the wrong zone beats never firing.
    let s = cron(
        "0 3 * * *",
        "Mars/Olympus_Mons",
        "instant",
        "2026-09-17T03:00:00Z",
    );
    assert_eq!(
        s.next_after(at("2026-09-17T03:00:05Z")).unwrap(),
        at("2026-09-18T03:00:00Z")
    );
}
