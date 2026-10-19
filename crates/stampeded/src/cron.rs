//! Cron expressions, evaluated with `jiff`.
//!
//! Supported: `minute hour day-of-month month day-of-week`, with `*`, `N`,
//! `A-B`, `*/S`, `A-B/S`, comma lists, and three-letter month/day names.

use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{ToSpan, Zoned};
use std::fmt;

/// How to resolve a local time made ambiguous or nonexistent by DST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DstPolicy {
    /// One firing per wall-clock slot: a fold fires once (first), a gap at its close.
    #[default]
    WallClock,
    /// Every matching instant fires, so an ambiguous 01:30 fires twice on a fall-back day.
    Instant,
}

impl DstPolicy {
    /// The name stored in `stampeded_schedules.dst_policy`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WallClock => "wall_clock",
            Self::Instant => "instant",
        }
    }
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
    /// True for explicit values (`3`, `3,15`, `1-5`), not `*` or a step; picks the DST default.
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
                // `5-7` is Fri..Sun: set Sunday here so 7 never makes the range inverted.
                if max == 6 && b.trim() == "7" {
                    bits |= 1;
                    (lo, 6)
                } else {
                    (lo, value_of(b, min, max, names)?)
                }
            } else {
                let v = value_of(range, min, max, names)?;
                // Vixie: `5/15` is 5, 20, 35, 50.
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
    // Names are listed from `min` upwards, so zipping assigns each its value.
    if let Some((_, v)) = names.iter().zip(min..=max).find(|(n, _)| **n == lower) {
        return Ok(v);
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

    /// The DST policy this expression most likely wants: named hours mean wall clock.
    /// Validate a schedule's expression and timezone; the policy to use when none is given.
    pub fn check(expr: &str, timezone: &str) -> Result<DstPolicy, String> {
        let cron = Self::parse(expr).map_err(|e| e.to_string())?;
        TimeZone::get(timezone).map_err(|e| format!("unknown timezone {timezone:?}: {e}"))?;
        Ok(cron.suggested_dst_policy())
    }

    pub fn suggested_dst_policy(&self) -> DstPolicy {
        if self.hour.explicit {
            DstPolicy::WallClock
        } else {
            DstPolicy::Instant
        }
    }

    /// Whether a civil date matches the day fields.
    ///
    /// Vixie rule: with both day fields restricted, either matches (`0 0 1 * mon`).
    #[expect(clippy::similar_names, reason = "dom/dow are cron's own field names")]
    fn day_matches(&self, date: Date) -> bool {
        // jiff's civil fields are i8 but never negative.
        if !self.month.matches(date.month().cast_unsigned()) {
            return false;
        }

        let dom_ok = self.dom.matches(date.day().cast_unsigned());
        // jiff: Monday = 1 ... Sunday = 7. Cron: Sunday = 0.
        let dow_ok = self
            .dow
            .matches((date.weekday().to_monday_one_offset() % 7).cast_unsigned());

        match (self.dom.wildcard, self.dow.wildcard) {
            (true, true) => true,
            (false, true) => dom_ok,
            (true, false) => dow_ok,
            (false, false) => dom_ok || dow_ok,
        }
    }

    /// The next firing strictly after `after`, in `tz`, under `policy`.
    ///
    /// `None` if nothing matches within four years, e.g. `0 0 30 feb *`.
    pub fn next_after(&self, after: &Zoned, tz: &TimeZone, policy: DstPolicy) -> Option<Zoned> {
        let local = after.with_time_zone(tz.clone());

        // The civil search below would walk past a fold's second instant.
        if policy == DstPolicy::Instant
            && let Some(z) = self.second_pass_of_fold(&local, tz)
        {
            return Some(z);
        }

        // Strictly after, or a schedule re-fires for the rest of its own minute.
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
    fn second_pass_of_fold(&self, after: &Zoned, tz: &TimeZone) -> Option<Zoned> {
        let dt = after.datetime();
        if !(self.day_matches(dt.date())
            && self.hour.matches(dt.hour().cast_unsigned())
            && self.minute.matches(dt.minute().cast_unsigned()))
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
        for hour in from.hour().cast_unsigned()..=23 {
            if !self.hour.matches(hour) {
                continue;
            }
            let first_minute = if hour == from.hour().cast_unsigned() {
                from.minute().cast_unsigned()
            } else {
                0
            };
            for minute in first_minute..=59 {
                if self.minute.matches(minute) {
                    return Time::new(hour.cast_signed(), minute.cast_signed(), 0, 0).ok();
                }
            }
        }
        None
    }
}

/// Civil date+time to an instant: a fold takes the earlier offset, a gap its close (Vixie).
fn resolve(date: Date, time: Time, tz: &TimeZone) -> Option<Zoned> {
    tz.to_ambiguous_zoned(DateTime::from_parts(date, time))
        .compatible()
        .ok()
}

#[cfg(test)]
mod tests;
