//! Cron parsing and next-occurrence tests.

use super::*;

fn zoned(s: &str) -> Zoned {
    s.parse().expect("parse zoned")
}

fn tz(name: &str) -> TimeZone {
    TimeZone::get(name).expect("tz")
}

fn next(expr: &str, from: &str, zone: &str, policy: DstPolicy) -> String {
    Cron::parse(expr)
        .expect("parse cron")
        .next_after(&zoned(from), &tz(zone), policy)
        .expect("a next occurrence")
        .strftime("%Y-%m-%d %H:%M %Z")
        .to_string()
}

// ---------------------------------------------------------------- parsing

#[test]
fn rejects_wrong_field_count() {
    assert!(Cron::parse("* * * *").is_err());
    assert!(Cron::parse("* * * * * *").is_err());
}

#[test]
fn rejects_out_of_range_and_nonsense() {
    assert!(Cron::parse("60 * * * *").is_err(), "minute 60");
    assert!(Cron::parse("* 24 * * *").is_err(), "hour 24");
    assert!(Cron::parse("* * 0 * *").is_err(), "day 0");
    assert!(Cron::parse("* * * 13 *").is_err(), "month 13");
    assert!(Cron::parse("*/0 * * * *").is_err(), "zero step");
    assert!(Cron::parse("5-1 * * * *").is_err(), "inverted range");
    assert!(Cron::parse("banana * * * *").is_err());
}

#[test]
fn accepts_names_and_step_forms() {
    assert!(Cron::parse("0 0 * jan mon").is_ok());
    assert!(
        Cron::parse("0 0 * JAN MON").is_ok(),
        "names are case-insensitive"
    );
    assert!(Cron::parse("*/15 * * * *").is_ok());
    assert!(Cron::parse("0-30/10 * * * *").is_ok());
    assert!(Cron::parse("1,2,3 * * * *").is_ok());
}

#[test]
fn sunday_is_both_zero_and_seven() {
    let a = Cron::parse("0 0 * * 0").unwrap();
    let b = Cron::parse("0 0 * * 7").unwrap();
    assert_eq!(a, b);
}

// ------------------------------------------------------------ scheduling

#[test]
fn every_minute_advances_one_minute() {
    assert_eq!(
        next(
            "* * * * *",
            "2026-03-10T10:15:30-04:00[America/New_York]",
            "America/New_York",
            DstPolicy::WallClock
        ),
        "2026-03-10 10:16 EDT"
    );
}

#[test]
fn is_strictly_after_so_it_does_not_refire_in_its_own_minute() {
    // On the boundary the next firing is the next slot, or next_run_at loops.
    assert_eq!(
        next(
            "0 * * * *",
            "2026-03-10T10:00:00-04:00[America/New_York]",
            "America/New_York",
            DstPolicy::WallClock
        ),
        "2026-03-10 11:00 EDT"
    );
}

#[test]
fn step_and_list_fields_pick_the_next_slot() {
    assert_eq!(
        next(
            "*/15 * * * *",
            "2026-03-10T10:16:00-04:00[America/New_York]",
            "America/New_York",
            DstPolicy::WallClock
        ),
        "2026-03-10 10:30 EDT"
    );
}

#[test]
fn rolls_into_the_next_day_and_month() {
    assert_eq!(
        next(
            "0 9 * * *",
            "2026-03-10T23:00:00-04:00[America/New_York]",
            "America/New_York",
            DstPolicy::WallClock
        ),
        "2026-03-11 09:00 EDT"
    );
    assert_eq!(
        next(
            "0 0 1 * *",
            "2026-03-15T00:00:00-04:00[America/New_York]",
            "America/New_York",
            DstPolicy::WallClock
        ),
        "2026-04-01 00:00 EDT"
    );
}

#[test]
fn dom_and_dow_together_match_either_not_both() {
    // Vixie: `1 * mon` means the 1st and also every Monday.
    let c = Cron::parse("0 0 1 * mon").unwrap();
    let t = tz("UTC");

    // 2026-06-01 is a Monday; 2026-06-08 is a Monday but not the 1st.
    let from = zoned("2026-06-02T00:00:00+00:00[UTC]");
    let n = c.next_after(&from, &t, DstPolicy::WallClock).unwrap();
    assert_eq!(
        n.strftime("%Y-%m-%d").to_string(),
        "2026-06-08",
        "Mondays should match"
    );

    // 2026-07-01 is a Wednesday: matches on day-of-month alone.
    let from = zoned("2026-06-30T12:00:00+00:00[UTC]");
    let n = c.next_after(&from, &t, DstPolicy::WallClock).unwrap();
    assert_eq!(n.strftime("%Y-%m-%d").to_string(), "2026-07-01");
}

#[test]
fn impossible_expression_returns_none_rather_than_hanging() {
    // 30 February never occurs. The search is bounded at four years.
    let c = Cron::parse("0 0 30 2 *").unwrap();
    assert!(
        c.next_after(
            &zoned("2026-01-01T00:00:00+00:00[UTC]"),
            &tz("UTC"),
            DstPolicy::WallClock
        )
        .is_none()
    );
}

#[test]
fn finds_29_february_within_the_bound() {
    let c = Cron::parse("0 0 29 2 *").unwrap();
    let n = c
        .next_after(
            &zoned("2027-03-01T00:00:00+00:00[UTC]"),
            &tz("UTC"),
            DstPolicy::WallClock,
        )
        .expect("2028 is a leap year");
    assert_eq!(n.strftime("%Y-%m-%d").to_string(), "2028-02-29");
}

// ------------------------------------------------------------------- DST

#[test]
fn wall_clock_fires_once_on_the_fall_back_day() {
    // 2026-11-01, America/New_York: 01:30 happens twice; a daily job runs once.
    let c = Cron::parse("30 1 * * *").unwrap();
    let t = tz("America/New_York");

    let first = c
        .next_after(
            &zoned("2026-11-01T00:00:00-04:00[America/New_York]"),
            &t,
            DstPolicy::WallClock,
        )
        .unwrap();
    assert_eq!(
        first.strftime("%Y-%m-%d %H:%M %Z").to_string(),
        "2026-11-01 01:30 EDT"
    );

    let second = c.next_after(&first, &t, DstPolicy::WallClock).unwrap();
    assert_eq!(
        second.strftime("%Y-%m-%d").to_string(),
        "2026-11-02",
        "wall_clock must not fire twice in the repeated hour"
    );
}

#[test]
fn instant_fires_twice_in_the_repeated_hour() {
    let c = Cron::parse("30 1 * * *").unwrap();
    let t = tz("America/New_York");
    let first = zoned("2026-11-01T01:30:00-04:00[America/New_York]");

    let second = c.next_after(&first, &t, DstPolicy::Instant).unwrap();
    assert_eq!(
        second.strftime("%Y-%m-%d %H:%M %Z").to_string(),
        "2026-11-01 01:30 EST"
    );
    assert_eq!(
        second.timestamp().as_second() - first.timestamp().as_second(),
        3600,
        "an hour of real time apart"
    );

    let third = c.next_after(&second, &t, DstPolicy::Instant).unwrap();
    assert_eq!(third.strftime("%Y-%m-%d").to_string(), "2026-11-02");
}

#[test]
fn spring_forward_runs_at_the_end_of_the_gap() {
    // 2026-03-08, America/New_York: 02:00-02:59 does not exist; 02:30 fires at 03:30.
    let c = Cron::parse("30 2 * * *").unwrap();
    let t = tz("America/New_York");
    let n = c
        .next_after(
            &zoned("2026-03-08T00:00:00-05:00[America/New_York]"),
            &t,
            DstPolicy::WallClock,
        )
        .expect("must still fire on a spring-forward day");
    assert_eq!(
        n.strftime("%Y-%m-%d %H:%M %Z").to_string(),
        "2026-03-08 03:30 EDT"
    );
}

#[test]
fn day_of_week_range_may_wrap_through_sunday() {
    let c = Cron::parse("0 0 * * 5-7").unwrap();
    assert!(Cron::parse("0 0 * * 5-7").is_ok());
    let t = tz("UTC");
    // 2026-06-03 is a Wednesday -> next match is Friday the 5th.
    let n = c
        .next_after(
            &zoned("2026-06-03T00:00:00+00:00[UTC]"),
            &t,
            DstPolicy::WallClock,
        )
        .unwrap();
    assert_eq!(n.strftime("%Y-%m-%d").to_string(), "2026-06-05");
    // ... then Sat, then Sun, then skip to Friday.
    let n = c
        .next_after(
            &zoned("2026-06-07T00:00:00+00:00[UTC]"),
            &t,
            DstPolicy::WallClock,
        )
        .unwrap();
    assert_eq!(
        n.strftime("%Y-%m-%d").to_string(),
        "2026-06-12",
        "Sunday matched, Mon-Thu did not"
    );
}

#[test]
fn hourly_schedules_are_unaffected_by_dst() {
    let c = Cron::parse("0 * * * *").unwrap();
    let t = tz("America/New_York");
    let n = c
        .next_after(
            &zoned("2026-11-01T01:00:00-04:00[America/New_York]"),
            &t,
            DstPolicy::Instant,
        )
        .unwrap();
    assert!(n.timestamp() > zoned("2026-11-01T01:00:00-04:00[America/New_York]").timestamp());
}

#[test]
fn utc_is_unaffected_by_any_of_this() {
    assert_eq!(
        next(
            "30 1 * * *",
            "2026-11-01T00:00:00+00:00[UTC]",
            "UTC",
            DstPolicy::WallClock
        ),
        "2026-11-01 01:30 UTC"
    );
}

// ------------------------------------------------------- default DST policy

#[test]
fn expressions_naming_their_hours_default_to_wall_clock() {
    for expr in ["0 3 * * *", "30 3,15 * * *", "0 9-17 * * mon-fri"] {
        assert_eq!(
            Cron::parse(expr).unwrap().suggested_dst_policy(),
            DstPolicy::WallClock,
            "{expr}"
        );
    }
}

#[test]
fn expressions_about_elapsed_time_default_to_instant() {
    // 13 runs on a 25-hour day, not 12.
    for expr in ["* * * * *", "*/30 * * * *", "0 */2 * * *", "0 0-23/3 * * *"] {
        assert_eq!(
            Cron::parse(expr).unwrap().suggested_dst_policy(),
            DstPolicy::Instant,
            "{expr}"
        );
    }
}

#[test]
fn check_validates_expression_and_timezone_and_suggests_a_policy() {
    assert_eq!(
        Cron::check("0 3 * * *", "Africa/Casablanca"),
        Ok(DstPolicy::WallClock)
    );
    assert_eq!(Cron::check("*/15 * * * *", "UTC"), Ok(DstPolicy::Instant));
    assert!(Cron::check("0 3 * *", "UTC").is_err());
    let unknown = Cron::check("0 3 * * *", "Mars/Olympus").unwrap_err();
    assert!(
        unknown.contains("Mars/Olympus") && !unknown.contains("cron"),
        "{unknown}"
    );
}
