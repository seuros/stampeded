//! Maintenance configuration invariants; the loops are covered by integration tests.

use super::*;

#[test]
fn dead_after_leaves_room_for_several_missed_heartbeats() {
    let m = MaintenanceConfig::default();
    let w = crate::worker::WorkerConfig::default();

    // A worker must survive a few missed heartbeats without being reaped.
    assert!(
        m.dead_after >= w.heartbeat_interval * 3,
        "dead_after {:?} too tight against heartbeat {:?}",
        m.dead_after,
        w.heartbeat_interval
    );
}

#[test]
fn reap_runs_more_often_than_the_dead_threshold() {
    let m = MaintenanceConfig::default();
    // Otherwise detection latency is dominated by the sweep, not dead_after.
    assert!(m.reap_interval < m.dead_after);
}

#[test]
fn advisory_lock_keys_are_distinct() {
    // A shared key would make the reaper and pruner take turns.
    assert_ne!(LOCK_REAPER, LOCK_PRUNER);
}
