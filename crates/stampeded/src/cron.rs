//! Cron expressions, evaluated with `jiff`.
//!
//! **Why not `croner` or `cron`?** Both are chrono-based. If a chrono crate
//! computes the next occurrence then jiff never touches the timezone
//! arithmetic, which is the entire reason to use jiff — and neither crate
//! implements the `wall_clock` vs `instant` DST split that `plans/11` specified.
//! `croner` also drags in a `derive_builder`/`darling`/`syn` proc-macro chain
//! for what is a small, well-specified grammar.
//!
//! So: the expression is parsed here, and every time computation goes through
//! jiff, whose `Disambiguation` makes the fall-back hour and the spring-forward
//! gap explicit rather than silently resolved.
//!
//! Supported: `minute hour day-of-month month day-of-week`, with `*`, `N`,
//! `A-B`, `*/S`, `A-B/S`, comma lists, and three-letter month/day names.

use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{ToSpan, Zoned};
use std::fmt;

/// How to resolve a local time that is ambiguous or does not exist because of
/// a DST transition (`plans/11`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DstPolicy {
    /// One firing per wall-clock slot, whatever the offset does. On a fall-back
    /// day an ambiguous 01:30 fires **once**, at the first of the two; on a
    /// spring-forward day a 02:30 that does not exist fires at 03:30, the
    /// instant the gap closes. "The 02:30 report" runs once a day, every day,
    /// which is what a human means by a daily schedule.
    #[default]
    WallClock,
    /// Treat the schedule as absolute elapsed time: every matching *instant*
    /// fires, so an ambiguous 01:30 fires **twice** on a fall-back day, an hour
    /// apart. Correct for "every N hours" work that should not skip an hour of
    /// real time because a local clock moved.
    Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronError(String);

impl fmt::Display for CronError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid cron expression: {}", self.0)
    }
}

impl std::error::Error for CronError {}

/// One parsed field: the set of values it matches.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    /// Bit per value, offset by `min`.
    bits: u64,
    min: u8,
    /// True for `*`, which day-of-month and day-of-week treat specially.
    wildcard: bool,
    /// True when the field names explicit values (`3`, `3,15`, `1-5`) rather
    /// than using `*` or a step. Distinguishes "at 03:00" from "every 2 hours",
    /// which is what picks the default DST policy.
    explicit: bool,
}

impl Field {
    fn matches(&self, value: u8) -> bool {
        value >= self.min && (self.bits >> (value - self.min)) & 1 == 1
    }

    fn parse(spec: &str, min: u8, max: u8, names: &[&str]) -> Result<Self, CronError> {
        let wildcard = spec == "*";
        let explicit = !spec.contains('*') && !spec.contains('/');
        let mut bits = 0u64;

        for part in spec.split(',') {
            let (range, step) = match part.split_once('/') {
                Some((r, s)) => {
                    let step: u8 = s
                        .parse()
                        .map_err(|_| CronError(format!("bad step {s:?}")))?;
                    if step == 0 {
                        return Err(CronError("step cannot be zero".into()));
                    }
                    (r, step)
                }
                None => (part, 1),
            };

            let (lo, hi) = if range == "*" {
                (min, max)
            } else if let Some((a, b)) = range.split_once('-') {
                let lo = value_of(a, min, max, names)?;
                // `5-7` in day-of-week is Fri..Sun. Normalising 7 to 0 first
                // would make it an inverted range and reject a valid crontab,
                // so Sunday is set here and the range stops at Saturday.
                if max == 6 && b.trim() == "7" {
                    bits |= 1;
                    (lo, 6)
                } else {
                    (lo, value_of(b, min, max, names)?)
                }
            } else {
                let v = value_of(range, min, max, names)?;
                // A bare value with a step means "from here to the end",
                // matching Vixie cron: `5/15` is 5, 20, 35, 50.
                if step > 1 { (v, max) } else { (v, v) }
            };

            if lo > hi {
                return Err(CronError(format!("range {lo}-{hi} is inverted")));
            }

            let mut v = lo;
            while v <= hi {
                bits |= 1 << (v - min);
                v += step;
            }
        }

        if bits == 0 {
            return Err(CronError(format!("field {spec:?} matches nothing")));
        }

        Ok(Self {
            bits,
            min,
            wildcard,
            explicit,
        })
    }
}

fn value_of(s: &str, min: u8, max: u8, names: &[&str]) -> Result<u8, CronError> {
    let lower = s.to_ascii_lowercase();
    if let Some(i) = names.iter().position(|n| *n == lower) {
        return Ok(min + i as u8);
    }
    let v: u8 = s
        .parse()
        .map_err(|_| CronError(format!("{s:?} is not a number or name")))?;
    // Sunday is both 0 and 7 in every cron implementation; normalise.
    let v = if max == 6 && v == 7 { 0 } else { v };
    if v < min || v > max {
        return Err(CronError(format!("{v} out of range {min}-{max}")));
    }
    Ok(v)
}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// A parsed cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    minute: Field,
    hour: Field,
    dom: Field,
    month: Field,
    dow: Field,
}

impl Cron {
    /// Parse a five-field expression: `minute hour day-of-month month day-of-week`.
    pub fn parse(expr: &str) -> Result<Self, CronError> {
        let f: Vec<&str> = expr.split_whitespace().collect();
        if f.len() != 5 {
            return Err(CronError(format!(
                "expected 5 fields, got {} in {expr:?}",
                f.len()
            )));
        }

        Ok(Self {
            minute: Field::parse(f[0], 0, 59, &[])?,
            hour: Field::parse(f[1], 0, 23, &[])?,
            dom: Field::parse(f[2], 1, 31, &[])?,
            month: Field::parse(f[3], 1, 12, &MONTHS)?,
            dow: Field::parse(f[4], 0, 6, &DAYS)?,
        })
    }

    /// The DST policy this expression most likely wants (`plans/11`).
    ///
    /// An expression that NAMES its hours — `0 3 * * *`, "the 03:00 report" —
    /// pins a wall-clock time, and a human expects one report on a fall-back
    /// day. An expression using `*` or a step in the hour field — `0 */2 * * *`,
    /// "every two hours" — is really about elapsed time and should run 13 times
    /// on a 25-hour day, not 12.
    ///
    /// A default, not a decision: the column is always settable.
    pub fn suggested_dst_policy(&self) -> DstPolicy {
        if self.hour.explicit {
            DstPolicy::WallClock
        } else {
            DstPolicy::Instant
        }
    }

    /// Whether a civil date matches the day fields.
    ///
    /// The Vixie rule, which surprises people often enough to be worth stating:
    /// when BOTH day-of-month and day-of-week are restricted, a date matches if
    /// it satisfies **either**, not both. `0 0 1 * mon` is "the 1st, and also
    /// every Monday".
    fn day_matches(&self, date: Date) -> bool {
        if !self.month.matches(date.month() as u8) {
            return false;
        }

        let dom_ok = self.dom.matches(date.day() as u8);
        // jiff: Monday = 1 ... Sunday = 7. Cron: Sunday = 0.
        let dow_ok = self
            .dow
            .matches((date.weekday().to_monday_one_offset() % 7) as u8);

        match (self.dom.wildcard, self.dow.wildcard) {
            (true, true) => true,
            (false, true) => dom_ok,
            (true, false) => dow_ok,
            (false, false) => dom_ok || dow_ok,
        }
    }

    /// The next firing strictly after `after`, in `tz`, under `policy`.
    ///
    /// Returns `None` if nothing matches within four years, which only happens
    /// for impossible expressions like `0 0 30 feb *`.
    pub fn next_after(&self, after: &Zoned, tz: &TimeZone, policy: DstPolicy) -> Option<Zoned> {
        let local = after.with_time_zone(tz.clone());

        // Under `instant`, a slot inside the repeated hour has TWO instants and
        // we may be sitting on the first of them. The civil-time search below
        // works in wall-clock terms and would walk straight past the second, so
        // it is picked off here first.
        if policy == DstPolicy::Instant
            && let Some(z) = self.second_pass_of_fold(&local, tz)
        {
            return Some(z);
        }

        // Strictly after: advance a minute and drop sub-minute precision, or a
        // schedule would re-fire for the remainder of its own minute.
        let start = local
            .datetime()
            .with()
            .second(0)
            .millisecond(0)
            .microsecond(0)
            .nanosecond(0)
            .build()
            .ok()?
            .checked_add(1.minute())
            .ok()?;

        let mut date = start.date();
        // Four years covers the worst legitimate case, 29 February.
        for day in 0..=1_461 {
            if self.day_matches(date) {
                let from_minute = if day == 0 {
                    start.time()
                } else {
                    Time::midnight()
                };
                if let Some(t) = self.first_time_on_or_after(from_minute) {
                    return resolve(date, t, tz);
                }
            }
            date = date.tomorrow().ok()?;
        }
        None
    }

    /// The second instant of an ambiguous slot, if `after` is the first one.
    ///
    /// Only reachable during a fall-back fold, i.e. once a year per zone.
    fn second_pass_of_fold(&self, after: &Zoned, tz: &TimeZone) -> Option<Zoned> {
        let dt = after.datetime();
        if !(self.day_matches(dt.date())
            && self.hour.matches(dt.hour() as u8)
            && self.minute.matches(dt.minute() as u8))
        {
            return None;
        }

        let AmbiguousOffset::Fold {
            before,
            after: later,
        } = tz.to_ambiguous_zoned(dt).offset()
        else {
            return None;
        };
        // Already on the later side: the fold is spent.
        if after.offset() != before {
            return None;
        }

        let candidate = dt
            .to_zoned(TimeZone::fixed(later))
            .ok()?
            .with_time_zone(tz.clone());
        (candidate.timestamp() > after.timestamp()).then_some(candidate)
    }

    fn first_time_on_or_after(&self, from: Time) -> Option<Time> {
        for hour in from.hour() as u8..=23 {
            if !self.hour.matches(hour) {
                continue;
            }
            let first_minute = if hour == from.hour() as u8 {
                from.minute() as u8
            } else {
                0
            };
            for minute in first_minute..=59 {
                if self.minute.matches(minute) {
                    return Time::new(hour as i8, minute as i8, 0, 0).ok();
                }
            }
        }
        None
    }
}

/// Turn a civil date+time into an instant.
///
/// This is where jiff earns its place. `compatible()` resolves a fold to the
/// EARLIER of the two offsets and a gap to the instant just after it closes —
/// so an ambiguous 01:30 fires once rather than twice, and a 02:30 that does
/// not exist fires at 03:30 rather than vanishing for the day. That is Vixie
/// cron's behaviour, and it is a documented guarantee here rather than whatever
/// a chrono `LocalResult` happened to collapse to.
fn resolve(date: Date, time: Time, tz: &TimeZone) -> Option<Zoned> {
    tz.to_ambiguous_zoned(DateTime::from_parts(date, time))
        .compatible()
        .ok()
}

#[cfg(test)]
mod tests;
