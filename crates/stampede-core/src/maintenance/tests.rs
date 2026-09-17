//! Maintenance configuration invariants.
//!
//! The loops themselves are covered by integration tests; what is worth
//! asserting here is the relationship between intervals, because getting it
//! wrong reaps healthy workers.

use super::*;

#[test]
fn dead_after_leaves_room_for_several_missed_heartbeats() {
    let m = MaintenanceConfig::default();
    let w = crate::worker::WorkerConfig::default();

    // A worker must be able to miss a few heartbeats -- a GC pause, a slow
    // query, a blip -- without having its in-flight jobs reclaimed underneath
    // it. Anything less than 3x is too tight.
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
    // Otherwise detection latency is dominated by the sweep interval rather
    // than by dead_after, and jobs sit orphaned longer than configured.
    assert!(m.reap_interval < m.dead_after);
}

#[test]
fn advisory_lock_keys_are_distinct() {
    // Sharing a key with another subsystem would silently serialise unrelated
    // work -- the reaper and pruner would take turns instead of running
    // independently.
    assert_ne!(LOCK_REAPER, LOCK_PRUNER);
}
