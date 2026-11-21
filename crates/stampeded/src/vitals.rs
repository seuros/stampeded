//! Host-vitals admission: stop claiming while disk or thermals are short.
//!
//! A worker stops claiming while its host is short of disk or under thermal pressure, and
//! resumes only once headroom has held for a chosen span. Every watched vital has its own [`Recovery`] from vitals-machines. A warning opens an
//! episode, and the episode is over only after headroom held, without a gap, for
//! [`VitalsConfig::headroom_for`]. A worker claims while every vital is `Healthy` or
//! `Recovered`; `Warning`, `Stabilizing`, `Unavailable` and `Blocked` hold claiming back.
//! Jobs already claimed keep running and heartbeating whatever the phase: vitals only
//! decide whether the next claim happens.
//!
//! An unreadable vital is never headroom. Inside an episode it holds claiming back
//! (`Unavailable`) and restarts the window. With no episode open it does not stop claiming:
//! nothing has been seen wrong yet, and stopping would turn a probe that cannot run (OS
//! thermal state on Linux is always unknown) into a worker that never claims. It is
//! reported as unreadable instead.

mod host;
mod watch;

pub(crate) use watch::VitalsWatch;

use crate::error::{Error, Result};
use serde::Deserialize;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use vitals_machines::{Recovery, Sample};

pub use vitals_machines::Phase;

/// How much free space a disk must keep.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FreeSpace {
    /// At least this many bytes available to the worker.
    Bytes(u64),
    /// At least this percentage of the filesystem's size, 0 to 100.
    Percent(f64),
}

impl FreeSpace {
    fn holds(self, available: u64, total: u64) -> bool {
        match self {
            Self::Bytes(bytes) => available >= bytes,
            Self::Percent(percent) => {
                total > 0 && available as f64 * 100.0 / total as f64 >= percent
            }
        }
    }

    /// Whether a filesystem of `total` bytes could ever have this much free.
    fn reachable(self, total: u64) -> bool {
        match self {
            Self::Bytes(bytes) => bytes <= total,
            Self::Percent(percent) => percent <= 100.0,
        }
    }

    fn validate(self, what: &str) -> Result<()> {
        match self {
            Self::Percent(p) if !p.is_finite() || !(0.0..=100.0).contains(&p) => Err(
                Error::InvalidConfig(format!("{what} must be between 0% and 100%, not {p}%")),
            ),
            _ => Ok(()),
        }
    }
}

impl fmt::Display for FreeSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(bytes) => f.write_str(&human_bytes(*bytes)),
            Self::Percent(percent) => write!(f, "{percent}%"),
        }
    }
}

/// A path whose filesystem must keep free space.
#[derive(Debug, Clone, PartialEq)]
pub struct DiskVital {
    /// Absolute; a path that does not exist yet is probed at its nearest existing ancestor.
    pub path: PathBuf,
    /// Below this, claiming stops.
    pub min_free: FreeSpace,
    /// Claiming resumes only once free space is back above this, for the headroom span.
    /// `None` resumes above `min_free`.
    pub resume_free: Option<FreeSpace>,
}

impl DiskVital {
    pub fn new(path: impl Into<PathBuf>, min_free: FreeSpace) -> Self {
        Self {
            path: path.into(),
            min_free,
            resume_free: None,
        }
    }

    /// A recovery margin: between `min_free` and this, claiming stays stopped.
    #[must_use]
    pub const fn resume_above(mut self, free: FreeSpace) -> Self {
        self.resume_free = Some(free);
        self
    }

    fn resume(&self) -> FreeSpace {
        self.resume_free.unwrap_or(self.min_free)
    }
}

/// What the host reads as; see [`Probe::custom`].
pub type ProbeFn = dyn Fn(&VitalsConfig) -> Readings + Send + Sync;

/// Where readings come from: the host by default.
#[derive(Clone, Default)]
pub struct Probe(Option<Arc<ProbeFn>>);

impl Probe {
    /// Readings from the host: free space with `statvfs`, the OS thermal state.
    pub const fn host() -> Self {
        Self(None)
    }

    /// Readings from elsewhere: a cgroup-aware disk check, or a script in tests. Called on a
    /// blocking thread once per probe interval; it must return one reading per configured
    /// disk, in order, and a thermal reading when thermal pressure is watched.
    pub fn custom(read: impl Fn(&VitalsConfig) -> Readings + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(read)))
    }

    pub(crate) fn read(&self, config: &VitalsConfig) -> Readings {
        self.0
            .as_ref()
            .map_or_else(|| host::read(config), |read| read(config))
    }
}

impl fmt::Debug for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "Probe::custom"
        } else {
            "Probe::host"
        })
    }
}

/// Which host vitals hold claiming back. Watches nothing by default.
#[derive(Debug, Clone)]
pub struct VitalsConfig {
    pub disks: Vec<DiskVital>,
    /// Stop claiming while the OS reports thermal pressure. macOS and FreeBSD report it;
    /// on Linux it is always unknown, so it never stops claiming there.
    pub thermal_pressure: bool,
    /// How long every vital must hold headroom, continuously, before claiming resumes.
    pub headroom_for: Duration,
    /// How often the host is probed. A silence of three intervals counts as a gap (the host
    /// slept, or the probe hung) and restarts the headroom window.
    pub probe_interval: Duration,
    pub probe: Probe,
}

impl Default for VitalsConfig {
    fn default() -> Self {
        Self {
            disks: Vec::new(),
            thermal_pressure: false,
            headroom_for: Duration::from_mins(2),
            probe_interval: Duration::from_secs(15),
            probe: Probe::host(),
        }
    }
}

impl VitalsConfig {
    /// Watch a disk.
    #[must_use]
    pub fn disk(mut self, disk: DiskVital) -> Self {
        self.disks.push(disk);
        self
    }

    /// Watch the OS thermal pressure.
    #[must_use]
    pub const fn thermal_pressure(mut self) -> Self {
        self.thermal_pressure = true;
        self
    }

    #[must_use]
    pub const fn headroom_for(mut self, span: Duration) -> Self {
        self.headroom_for = span;
        self
    }

    #[must_use]
    pub const fn probe_interval(mut self, interval: Duration) -> Self {
        self.probe_interval = interval;
        self
    }

    #[must_use]
    pub fn probe(mut self, probe: Probe) -> Self {
        self.probe = probe;
        self
    }

    /// Whether anything is watched; a config watching nothing never probes.
    pub const fn is_enabled(&self) -> bool {
        !self.disks.is_empty() || self.thermal_pressure
    }

    pub fn validate(&self) -> Result<()> {
        if self.probe_interval.is_zero() {
            return Err(Error::InvalidConfig(
                "vitals probe_interval must be above zero".into(),
            ));
        }
        for disk in &self.disks {
            if !disk.path.is_absolute() {
                return Err(Error::InvalidConfig(format!(
                    "vitals disk path {} must be absolute",
                    disk.path.display()
                )));
            }
            disk.min_free.validate("min_free")?;
            if let Some(resume) = disk.resume_free {
                resume.validate("resume_free")?;
            }
        }
        Ok(())
    }

    /// The silence that breaks continuity: three probe intervals.
    fn max_gap(&self) -> Duration {
        self.probe_interval.saturating_mul(3)
    }

    /// The JSON the C ABI and the Ruby gem pass, durations in seconds:
    ///
    /// ```json
    /// { "disks": [{ "path": "/var/lib/app", "min_free": "10%", "resume_free": 20000000000 }],
    ///   "thermal_pressure": true, "headroom": 120, "probe_interval": 15 }
    /// ```
    ///
    /// `min_free` and `resume_free` are a byte count or a percentage string.
    pub fn from_json(json: &str) -> Result<Self> {
        let spec: Spec =
            serde_json::from_str(json).map_err(|e| Error::InvalidConfig(format!("vitals: {e}")))?;
        let mut config = Self {
            thermal_pressure: spec.thermal_pressure,
            ..Self::default()
        };
        if let Some(secs) = spec.headroom {
            config.headroom_for = seconds(secs, "headroom")?;
        }
        if let Some(secs) = spec.probe_interval {
            config.probe_interval = seconds(secs, "probe_interval")?;
        }
        for disk in spec.disks {
            config.disks.push(DiskVital {
                path: disk.path,
                min_free: disk.min_free.free_space("min_free")?,
                resume_free: disk
                    .resume_free
                    .map(|r| r.free_space("resume_free"))
                    .transpose()?,
            });
        }
        config.validate()?;
        Ok(config)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    #[serde(default)]
    disks: Vec<DiskSpec>,
    #[serde(default)]
    thermal_pressure: bool,
    headroom: Option<f64>,
    probe_interval: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskSpec {
    path: PathBuf,
    min_free: Amount,
    resume_free: Option<Amount>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Amount {
    Bytes(u64),
    Text(String),
}

impl Amount {
    fn free_space(self, what: &str) -> Result<FreeSpace> {
        let invalid = |shown: &str| {
            Error::InvalidConfig(format!(
                "vitals {what} must be a byte count or a percentage like \"10%\", not {shown:?}"
            ))
        };
        match self {
            Self::Bytes(bytes) => Ok(FreeSpace::Bytes(bytes)),
            Self::Text(text) => text
                .trim()
                .strip_suffix('%')
                .and_then(|p| p.trim().parse::<f64>().ok())
                .map(FreeSpace::Percent)
                .ok_or_else(|| invalid(&text)),
        }
    }
}

fn seconds(secs: f64, what: &str) -> Result<Duration> {
    Duration::try_from_secs_f64(secs)
        .map_err(|_| Error::InvalidConfig(format!("vitals {what} must be seconds, not {secs}")))
}

/// One watched vital.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Vital {
    Disk(PathBuf),
    Thermal,
}

impl fmt::Display for Vital {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disk(path) => write!(f, "disk:{}", path.display()),
            Self::Thermal => f.write_str("thermal"),
        }
    }
}

/// Free space on one disk, as probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskReading {
    Space {
        available: u64,
        total: u64,
    },
    /// The probe failed; never zero free space, never plenty.
    Unreadable(String),
}

/// The OS thermal state, as probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThermalReading {
    Normal,
    Pressure,
    Critical,
    /// The OS does not say, or could not be asked; not evidence of cooling.
    Unknown,
}

/// One probe of the host: a reading per configured disk, in order, and the thermal state
/// when it is watched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Readings {
    pub disks: Vec<DiskReading>,
    pub thermal: Option<ThermalReading>,
}

impl Readings {
    /// Every watched vital unreadable, for `reason`.
    pub(crate) fn unreadable(config: &VitalsConfig, reason: &str) -> Self {
        Self {
            disks: config
                .disks
                .iter()
                .map(|_| DiskReading::Unreadable(reason.to_string()))
                .collect(),
            thermal: config.thermal_pressure.then_some(ThermalReading::Unknown),
        }
    }
}

/// Where one vital stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VitalState {
    pub vital: Vital,
    pub phase: Phase,
    /// Whether the last probe could read it.
    pub readable: bool,
    /// The last reading against its threshold, in words.
    pub detail: String,
}

/// Whether the worker claims, and why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VitalsStatus {
    pub admitting: bool,
    /// The phase of the vital holding claiming back, or with none, `Recovered` if any
    /// vital recovered from an episode and `Healthy` otherwise.
    pub phase: Phase,
    /// The vital holding claiming back; the most severe when several do.
    pub holding: Option<Vital>,
    /// Why claiming is held back, in words.
    pub reason: Option<String>,
    pub vitals: Vec<VitalState>,
}

impl VitalsStatus {
    /// Same admission, phases and readability; details (free bytes, seconds held) aside.
    pub fn same_state(&self, other: &Self) -> bool {
        self.admitting == other.admitting
            && self.phase == other.phase
            && self.holding == other.holding
            && self.vitals.len() == other.vitals.len()
            && self
                .vitals
                .iter()
                .zip(&other.vitals)
                .all(|(a, b)| a.vital == b.vital && a.phase == b.phase && a.readable == b.readable)
    }

    /// The `vitals` key of the worker row's `metadata`.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "admitting": self.admitting,
            "phase": phase_name(self.phase),
            "vital": self.holding.as_ref().map(ToString::to_string),
            "reason": self.reason,
            "vitals": self.vitals.iter().map(|v| serde_json::json!({
                "vital": v.vital.to_string(),
                "phase": phase_name(v.phase),
                "readable": v.readable,
                "detail": v.detail,
            })).collect::<Vec<_>>(),
        })
    }
}

/// The phase as the worker row and logs spell it.
pub const fn phase_name(phase: Phase) -> &'static str {
    match phase {
        Phase::Healthy => "healthy",
        Phase::Warning => "warning",
        Phase::Stabilizing => "stabilizing",
        Phase::Recovered => "recovered",
        Phase::Unavailable => "unavailable",
        Phase::Blocked => "blocked",
    }
}

/// The `stampeded.vitals.phase` gauge: 0 healthy, 1 recovered (both claim), 2 stabilizing,
/// 3 unavailable, 4 blocked, 5 warning.
pub const fn phase_code(phase: Phase) -> u8 {
    match phase {
        Phase::Healthy => 0,
        Phase::Recovered => 1,
        Phase::Stabilizing => 2,
        Phase::Unavailable => 3,
        Phase::Blocked => 4,
        Phase::Warning => 5,
    }
}

const fn claims(phase: Phase) -> bool {
    matches!(phase, Phase::Healthy | Phase::Recovered)
}

#[derive(Debug, Clone)]
struct Watched {
    vital: Vital,
    disk: Option<DiskVital>,
    recovery: Recovery,
    readable: bool,
    detail: String,
}

/// The admission decision, without a clock or probes of its own: feed it readings and the
/// time, from a clock that only goes forward and keeps counting while the host sleeps.
#[derive(Debug, Clone)]
pub struct VitalsGate {
    headroom_for: Duration,
    watched: Vec<Watched>,
}

impl VitalsGate {
    pub fn new(config: &VitalsConfig) -> Self {
        let max_gap = config.max_gap();
        let watch = |vital, disk| Watched {
            vital,
            disk,
            recovery: Recovery::new(max_gap),
            readable: true,
            detail: "not probed yet".into(),
        };
        let mut watched: Vec<Watched> = config
            .disks
            .iter()
            .map(|d| watch(Vital::Disk(d.path.clone()), Some(d.clone())))
            .collect();
        if config.thermal_pressure {
            watched.push(watch(Vital::Thermal, None));
        }
        Self {
            headroom_for: config.headroom_for,
            watched,
        }
    }

    /// Feed one probe taken at `now`, and answer whether to claim.
    pub fn observe(&mut self, now: Duration, readings: &Readings) -> VitalsStatus {
        let headroom = Sample::Headroom {
            required: self.headroom_for,
        };
        let mut disks = readings.disks.iter();
        for w in &mut self.watched {
            let (sample, detail) = w.disk.as_ref().map_or_else(
                || thermal_sample(readings.thermal, headroom),
                |disk| disk_sample(disk, disks.next(), headroom),
            );
            w.readable = sample != Sample::Unavailable;
            w.detail = detail;
            if let Err(refusal) = w.recovery.observe(now, sample) {
                // The machine is left as it was; the next probe tries again.
                warn!(vital = %w.vital, ?refusal, "host vitals refused an observation");
            }
            if w.recovery.phase() == Phase::Stabilizing
                && let Some(held) = w.recovery.stable_for()
            {
                w.detail = format!(
                    "{}; headroom held {}s of {}s",
                    w.detail,
                    held.as_secs(),
                    self.headroom_for.as_secs()
                );
            }
        }
        self.status()
    }

    pub fn status(&self) -> VitalsStatus {
        let vitals: Vec<VitalState> = self
            .watched
            .iter()
            .map(|w| VitalState {
                vital: w.vital.clone(),
                phase: w.recovery.phase(),
                readable: w.readable,
                detail: w.detail.clone(),
            })
            .collect();
        let holding = vitals
            .iter()
            .filter(|v| !claims(v.phase))
            .max_by_key(|v| phase_code(v.phase));
        let phase = holding.map_or_else(
            || {
                if vitals.iter().any(|v| v.phase == Phase::Recovered) {
                    Phase::Recovered
                } else {
                    Phase::Healthy
                }
            },
            |v| v.phase,
        );
        VitalsStatus {
            admitting: holding.is_none(),
            phase,
            reason: holding.map(|v| format!("{} {}: {}", v.vital, phase_name(v.phase), v.detail)),
            holding: holding.map(|v| v.vital.clone()),
            vitals,
        }
    }
}

/// Below `min_free` is a warning; between it and `resume_free` is short; above, headroom.
fn disk_sample(
    disk: &DiskVital,
    reading: Option<&DiskReading>,
    headroom: Sample,
) -> (Sample, String) {
    let (available, total) = match reading {
        Some(DiskReading::Space { available, total }) => (*available, *total),
        Some(DiskReading::Unreadable(reason)) => {
            return (Sample::Unavailable, format!("unreadable: {reason}"));
        }
        None => return (Sample::Unavailable, "unreadable: not probed".into()),
    };
    let free = format!("{} free of {}", human_bytes(available), human_bytes(total));
    let resume = disk.resume();
    if !disk.min_free.holds(available, total) {
        (
            Sample::Warning,
            format!("{free}, under the {} minimum", disk.min_free),
        )
    } else if !resume.reachable(total) {
        (
            Sample::Blocked,
            format!("{free}; resuming above {resume} cannot happen on this filesystem"),
        )
    } else if !resume.holds(available, total) {
        (
            Sample::Short,
            format!("{free}, not yet back above {resume}"),
        )
    } else {
        (headroom, free)
    }
}

fn thermal_sample(reading: Option<ThermalReading>, headroom: Sample) -> (Sample, String) {
    match reading {
        Some(ThermalReading::Pressure) => {
            (Sample::Warning, "the OS reports thermal pressure".into())
        }
        Some(ThermalReading::Critical) => (
            Sample::Warning,
            "the OS reports critical thermal pressure".into(),
        ),
        Some(ThermalReading::Normal) => (headroom, "the OS reports no thermal pressure".into()),
        Some(ThermalReading::Unknown) | None => (
            Sample::Unavailable,
            "unreadable: the OS does not report a thermal state".into(),
        ),
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[cfg(test)]
mod tests;
