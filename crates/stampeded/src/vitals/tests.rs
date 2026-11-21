//! The gate on a scripted clock and scripted readings; no probe runs except where named.

use super::watch::HostClock;
use super::*;
use std::assert_matches;
use std::time::{Instant, SystemTime};

mod postgres;

const GIB: u64 = 1024 * 1024 * 1024;
const PATH: &str = "/var/lib/stampeded";

fn s(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

/// One disk: under 10 GiB stops claiming; headroom must hold 120 s; probes every 10 s.
fn disk_config() -> VitalsConfig {
    VitalsConfig::default()
        .disk(DiskVital::new(PATH, FreeSpace::Bytes(10 * GIB)))
        .headroom_for(s(120))
        .probe_interval(s(10))
}

fn free(gib: u64) -> Readings {
    Readings {
        disks: vec![DiskReading::Space {
            available: gib * GIB,
            total: 100 * GIB,
        }],
        thermal: None,
    }
}

fn unreadable() -> Readings {
    Readings {
        disks: vec![DiskReading::Unreadable("EIO".into())],
        thermal: None,
    }
}

/// Feed `readings` every 10 s over `from..=to`; the last status.
fn hold(gate: &mut VitalsGate, from: u64, to: u64, readings: &Readings) -> VitalsStatus {
    let mut status = gate.status();
    for at in (from..=to).step_by(10) {
        status = gate.observe(s(at), readings);
    }
    status
}

#[test]
fn plenty_of_room_claims_and_reports_healthy() {
    let mut gate = VitalsGate::new(&disk_config());
    let status = gate.observe(s(0), &free(50));
    assert!(status.admitting);
    assert_eq!(status.phase, Phase::Healthy);
    assert_eq!(status.holding, None);
    assert_eq!(status.reason, None);
}

#[test]
fn a_warning_stops_claiming_and_names_the_vital() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(50));
    let status = gate.observe(s(10), &free(5));

    assert!(!status.admitting);
    assert_eq!(status.phase, Phase::Warning);
    assert_eq!(status.holding, Some(Vital::Disk(PATH.into())));
    let reason = status.reason.expect("a reason");
    assert!(
        reason.starts_with("disk:/var/lib/stampeded warning: 5.0 GiB free"),
        "{reason}"
    );
    assert!(reason.contains("under the 10.0 GiB minimum"), "{reason}");
}

#[test]
fn claiming_resumes_only_after_headroom_held_for_the_whole_span() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(5));

    // Back above the minimum at 10 s: the window starts, claiming stays stopped.
    let status = hold(&mut gate, 10, 120, &free(50));
    assert!(!status.admitting, "held 110 s of 120 s");
    assert_eq!(status.phase, Phase::Stabilizing);
    assert!(
        status
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("headroom held 110s of 120s")),
        "{:?}",
        status.reason
    );

    let status = gate.observe(s(130), &free(50));
    assert!(status.admitting);
    assert_eq!(status.phase, Phase::Recovered);
}

#[test]
fn a_silence_longer_than_three_probes_restarts_stabilization() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(5));
    gate.observe(s(10), &free(50));

    // Nobody watched from 10 s to 100 s (the host slept): that is not holding.
    let status = gate.observe(s(100), &free(50));
    assert_eq!(status.phase, Phase::Stabilizing);
    assert!(
        status.vitals[0]
            .detail
            .ends_with("headroom held 0s of 120s"),
        "{}",
        status.vitals[0].detail
    );
    // 130 s would have recovered a window started at 10 s.
    assert!(!hold(&mut gate, 110, 210, &free(50)).admitting);
    assert!(gate.observe(s(220), &free(50)).admitting);
}

#[test]
fn falling_back_under_the_minimum_while_stabilizing_starts_over() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(5));
    hold(&mut gate, 10, 100, &free(50));

    assert_eq!(gate.observe(s(110), &free(5)).phase, Phase::Warning);
    assert!(!hold(&mut gate, 120, 230, &free(50)).admitting);
    assert!(gate.observe(s(240), &free(50)).admitting);
}

#[test]
fn inside_the_recovery_margin_is_short_and_keeps_claiming_stopped() {
    let config = VitalsConfig {
        disks: vec![
            DiskVital::new(PATH, FreeSpace::Bytes(10 * GIB))
                .resume_above(FreeSpace::Bytes(20 * GIB)),
        ],
        ..disk_config()
    };
    let mut gate = VitalsGate::new(&config);
    gate.observe(s(0), &free(5));

    // Above the minimum but not the margin: still the warning, for as long as it lasts.
    let status = hold(&mut gate, 10, 500, &free(15));
    assert!(!status.admitting);
    assert_eq!(status.phase, Phase::Warning);
    assert!(
        status.vitals[0]
            .detail
            .contains("not yet back above 20.0 GiB"),
        "{}",
        status.vitals[0].detail
    );

    assert!(!hold(&mut gate, 510, 620, &free(25)).admitting);
    assert!(gate.observe(s(630), &free(25)).admitting);
}

#[test]
fn a_margin_the_filesystem_cannot_reach_is_blocked() {
    let config = VitalsConfig {
        disks: vec![
            DiskVital::new(PATH, FreeSpace::Bytes(10 * GIB))
                .resume_above(FreeSpace::Bytes(500 * GIB)),
        ],
        ..disk_config()
    };
    let mut gate = VitalsGate::new(&config);
    gate.observe(s(0), &free(5));

    let status = hold(&mut gate, 10, 1_000, &free(90));
    assert!(!status.admitting);
    assert_eq!(status.phase, Phase::Blocked);
    assert!(
        status
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("cannot happen")),
        "{:?}",
        status.reason
    );
}

#[test]
fn an_unreadable_vital_with_no_episode_open_does_not_stop_claiming() {
    let mut gate = VitalsGate::new(&disk_config());
    let status = gate.observe(s(0), &unreadable());

    assert!(status.admitting, "nothing has been seen wrong yet");
    assert_eq!(status.phase, Phase::Healthy);
    assert!(!status.vitals[0].readable);
    assert_eq!(status.vitals[0].detail, "unreadable: EIO");
}

#[test]
fn an_unreadable_vital_inside_an_episode_holds_claiming_and_is_never_headroom() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(5));
    hold(&mut gate, 10, 100, &free(50));

    let status = gate.observe(s(110), &unreadable());
    assert!(!status.admitting);
    assert_eq!(status.phase, Phase::Unavailable);
    assert!(!status.vitals[0].readable);

    // Readable again: the window starts from zero, not from 10 s.
    let status = gate.observe(s(120), &free(50));
    assert_eq!(status.phase, Phase::Stabilizing);
    assert!(!hold(&mut gate, 130, 230, &free(50)).admitting);
    assert!(gate.observe(s(240), &free(50)).admitting);
}

#[test]
fn a_missing_disk_reading_is_unreadable() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(5));
    let status = gate.observe(s(10), &Readings::default());
    assert_eq!(status.phase, Phase::Unavailable);
    assert_eq!(status.vitals[0].detail, "unreadable: not probed");
}

#[test]
fn thermal_pressure_stops_claiming_until_the_os_reports_none_for_the_span() {
    let config = VitalsConfig::default()
        .thermal_pressure()
        .headroom_for(s(60))
        .probe_interval(s(10));
    let mut gate = VitalsGate::new(&config);
    let thermal = |t| Readings {
        disks: Vec::new(),
        thermal: Some(t),
    };

    assert!(
        gate.observe(s(0), &thermal(ThermalReading::Normal))
            .admitting
    );
    let status = gate.observe(s(10), &thermal(ThermalReading::Critical));
    assert!(!status.admitting);
    assert_eq!(status.holding, Some(Vital::Thermal));

    assert!(!hold(&mut gate, 20, 70, &thermal(ThermalReading::Normal)).admitting);
    assert!(
        gate.observe(s(80), &thermal(ThermalReading::Normal))
            .admitting
    );
}

#[test]
fn an_os_that_never_reports_thermals_does_not_hold_back_a_disk_recovery() {
    // Linux: the OS thermal state is always unknown.
    let config = disk_config().thermal_pressure();
    let mut gate = VitalsGate::new(&config);
    let with_thermal = |gib| Readings {
        thermal: Some(ThermalReading::Unknown),
        ..free(gib)
    };

    let status = gate.observe(s(0), &with_thermal(50));
    assert!(status.admitting);
    assert!(!status.vitals[1].readable);

    gate.observe(s(10), &with_thermal(5));
    assert!(!hold(&mut gate, 20, 130, &with_thermal(50)).admitting);
    let status = gate.observe(s(140), &with_thermal(50));
    assert!(status.admitting);
    assert_eq!(status.phase, Phase::Recovered);
}

#[test]
fn the_most_severe_vital_is_the_one_reported() {
    let config = VitalsConfig::default()
        .disk(DiskVital::new("/a", FreeSpace::Bytes(10 * GIB)))
        .disk(DiskVital::new("/b", FreeSpace::Bytes(10 * GIB)))
        .probe_interval(s(10));
    let mut gate = VitalsGate::new(&config);
    let space = |gib| DiskReading::Space {
        available: gib * GIB,
        total: 100 * GIB,
    };
    gate.observe(
        s(0),
        &Readings {
            disks: vec![space(5), space(50)],
            thermal: None,
        },
    );
    // /a stabilizing, /b in warning: /b is why nothing is claimed.
    let status = gate.observe(
        s(10),
        &Readings {
            disks: vec![space(50), space(5)],
            thermal: None,
        },
    );
    assert_eq!(status.phase, Phase::Warning);
    assert_eq!(status.holding, Some(Vital::Disk("/b".into())));
    assert_eq!(status.vitals[0].phase, Phase::Stabilizing);
}

#[test]
fn percent_thresholds_compare_against_the_filesystem_size() {
    let config = VitalsConfig::default()
        .disk(DiskVital::new(PATH, FreeSpace::Percent(10.0)))
        .probe_interval(s(10));
    let mut gate = VitalsGate::new(&config);
    assert!(gate.observe(s(0), &free(10)).admitting, "exactly 10% holds");
    assert!(!gate.observe(s(10), &free(9)).admitting);

    let empty = Readings {
        disks: vec![DiskReading::Space {
            available: 0,
            total: 0,
        }],
        thermal: None,
    };
    assert!(
        !gate.observe(s(20), &empty).admitting,
        "a zero-size filesystem has no free percentage"
    );
}

#[test]
fn a_clock_regression_changes_nothing() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(100), &free(5));
    let status = gate.observe(s(50), &free(50));
    assert_eq!(status.phase, Phase::Warning);
}

#[test]
fn same_state_ignores_details_but_not_phases() {
    let mut gate = VitalsGate::new(&disk_config());
    gate.observe(s(0), &free(5));
    let a = gate.observe(s(10), &free(50));
    let b = gate.observe(s(20), &free(60));
    assert_ne!(a, b, "the details moved");
    assert!(a.same_state(&b));

    let c = hold(&mut gate, 30, 200, &free(50));
    assert!(!a.same_state(&c));
}

#[test]
fn the_worker_row_json_names_phase_vital_and_reason() {
    let mut gate = VitalsGate::new(&disk_config());
    let json = gate.observe(s(0), &free(5)).to_json();
    assert_eq!(json["admitting"], false);
    assert_eq!(json["phase"], "warning");
    assert_eq!(json["vital"], "disk:/var/lib/stampeded");
    assert!(
        json["reason"]
            .as_str()
            .is_some_and(|r| r.contains("minimum"))
    );
    assert_eq!(json["vitals"][0]["phase"], "warning");
    assert_eq!(json["vitals"][0]["readable"], true);

    let json = hold(&mut gate, 10, 200, &free(50)).to_json();
    assert_eq!(json["admitting"], true);
    assert_eq!(json["phase"], "recovered");
    assert!(json["vital"].is_null());
}

#[test]
fn phase_codes_put_every_claiming_phase_below_every_holding_one() {
    let all = [
        Phase::Healthy,
        Phase::Recovered,
        Phase::Stabilizing,
        Phase::Unavailable,
        Phase::Blocked,
        Phase::Warning,
    ];
    for phase in all {
        assert_eq!(claims(phase), phase_code(phase) < 2, "{phase:?}");
    }
}

#[test]
fn a_config_watching_nothing_is_disabled() {
    assert!(!VitalsConfig::default().is_enabled());
    assert!(disk_config().is_enabled());
    assert!(VitalsConfig::default().thermal_pressure().is_enabled());
}

#[test]
fn validation_rejects_what_cannot_work() {
    let bad = [
        disk_config().probe_interval(Duration::ZERO),
        VitalsConfig::default().disk(DiskVital::new("relative", FreeSpace::Bytes(1))),
        VitalsConfig::default().disk(DiskVital::new(PATH, FreeSpace::Percent(101.0))),
        VitalsConfig::default().disk(DiskVital::new(PATH, FreeSpace::Percent(f64::NAN))),
        VitalsConfig::default()
            .disk(DiskVital::new(PATH, FreeSpace::Bytes(1)).resume_above(FreeSpace::Percent(-1.0))),
    ];
    for config in bad {
        assert_matches!(
            config.validate(),
            Err(Error::InvalidConfig(_)),
            "{config:?}"
        );
    }
    assert!(disk_config().validate().is_ok());
}

#[test]
fn json_config_takes_bytes_percentages_and_seconds() {
    let config = VitalsConfig::from_json(
        r#"{ "disks": [
                { "path": "/var/lib/app", "min_free": "10%", "resume_free": 20000000000 },
                { "path": "/tmp", "min_free": 1000 }
             ],
             "thermal_pressure": true, "headroom": 90.5, "probe_interval": 5 }"#,
    )
    .expect("valid");

    assert_eq!(
        config.disks,
        vec![
            DiskVital::new("/var/lib/app", FreeSpace::Percent(10.0))
                .resume_above(FreeSpace::Bytes(20_000_000_000)),
            DiskVital::new("/tmp", FreeSpace::Bytes(1000)),
        ]
    );
    assert!(config.thermal_pressure);
    assert_eq!(config.headroom_for, Duration::from_millis(90_500));
    assert_eq!(config.probe_interval, s(5));

    let defaults = VitalsConfig::from_json("{}").expect("valid");
    assert!(!defaults.is_enabled());
    assert_eq!(defaults.headroom_for, s(120));
}

#[test]
fn json_config_rejects_typos_and_bad_amounts() {
    for json in [
        r#"{ "thermal": true }"#,
        r#"{ "disks": [{ "path": "/x", "min_free": "10GB" }] }"#,
        r#"{ "disks": [{ "path": "/x", "min_free": -1 }] }"#,
        r#"{ "disks": [{ "path": "x", "min_free": 1 }] }"#,
        r#"{ "headroom": -1 }"#,
        "not json",
    ] {
        assert_matches!(
            VitalsConfig::from_json(json),
            Err(Error::InvalidConfig(_)),
            "{json}"
        );
    }
}

#[test]
fn the_clock_counts_time_the_host_spent_asleep() {
    let wall = SystemTime::UNIX_EPOCH + s(1_000_000);
    let mut clock = HostClock::starting(Instant::now(), wall);

    assert_eq!(clock.advance(s(10), wall + s(10)), s(10));
    // The monotonic clock stood still for an hour of wall time: a suspend.
    assert_eq!(clock.advance(s(20), wall + s(3_620)), s(3_620));
    // A wall clock stepped back adds nothing and takes nothing away.
    assert_eq!(clock.advance(s(30), wall + s(100)), s(3_630));
    // Ordinary drift under the tolerance is not a sleep.
    assert_eq!(clock.advance(s(40), wall + s(111)), s(3_640));
    // Never backwards, whatever the monotonic reading says.
    assert_eq!(clock.advance(s(35), wall + s(111)), s(3_640));
}

#[test]
fn thermal_states_map_unknown_to_unknown() {
    use vitals_machines::probe::ThermalState;
    assert_eq!(
        host::thermal_reading(ThermalState::Normal),
        ThermalReading::Normal
    );
    assert_eq!(
        host::thermal_reading(ThermalState::Warning),
        ThermalReading::Pressure
    );
    assert_eq!(
        host::thermal_reading(ThermalState::Critical),
        ThermalReading::Critical
    );
    assert_eq!(
        host::thermal_reading(ThermalState::Unknown),
        ThermalReading::Unknown
    );
}

/// The one real probe: `statvfs` on the temp dir, and a relative path refused.
#[test]
fn the_host_probe_reads_a_real_disk_and_reports_a_bad_path_as_unreadable() {
    let config = VitalsConfig::default()
        .disk(DiskVital::new(std::env::temp_dir(), FreeSpace::Bytes(1)))
        .disk(DiskVital::new("relative/path", FreeSpace::Bytes(1)));
    let readings = Probe::host().read(&config);

    let DiskReading::Space { available, total } = &readings.disks[0] else {
        panic!("the temp dir is readable: {:?}", readings.disks[0]);
    };
    assert!(total >= available && *total > 0);
    assert_matches!(&readings.disks[1], DiskReading::Unreadable(_));
    assert_eq!(readings.thermal, None, "thermals are not watched");
}
