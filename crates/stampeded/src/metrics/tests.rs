//! Recorder plumbing and the stability of the published names.

use super::*;
use std::sync::{Arc, Mutex};

/// name, rendered value, labels.
type Seen = Vec<(String, String, Vec<Label>)>;

#[derive(Default)]
struct Capture(Arc<Mutex<Seen>>);

impl Recorder for Capture {
    fn count(&self, name: &'static str, v: u64, l: &[Label]) {
        self.0
            .lock()
            .unwrap()
            .push((name.into(), format!("count:{v}"), l.to_vec()));
    }
    fn gauge(&self, name: &'static str, v: f64, l: &[Label]) {
        self.0
            .lock()
            .unwrap()
            .push((name.into(), format!("gauge:{v}"), l.to_vec()));
    }
    fn duration(&self, name: &'static str, v: Duration, l: &[Label]) {
        self.0
            .lock()
            .unwrap()
            .push((name.into(), format!("dur:{}", v.as_millis()), l.to_vec()));
    }
}

#[test]
fn measurements_reach_the_recorder_with_their_labels() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let m = Metrics::new(Capture(seen.clone()));

    m.count(names::JOBS_COMPLETED, 1, &[label("queue", "mail")]);
    m.duration(names::JOB_DURATION_SECONDS, Duration::from_millis(250), &[]);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].0, "stampeded.jobs.completed");
    assert_eq!(seen[0].2, vec![("queue", "mail".to_string())]);
    assert_eq!(seen[1].1, "dur:250");
}

#[test]
fn the_default_recorder_swallows_everything() {
    let m = Metrics::default();
    m.count(names::JOBS_FAILED, 1, &[]);
    m.gauge(names::QUEUE_DEPTH, 3.0, &[]);
    m.duration(names::CLAIM_DURATION_SECONDS, Duration::from_secs(1), &[]);
}

#[test]
fn metric_names_are_namespaced_and_stable() {
    // Metric names are an API; this makes a rename loud.
    let all = [
        names::QUEUE_DEPTH,
        names::QUEUE_OLDEST_AGE_SECONDS,
        names::JOB_QUEUE_TIME_SECONDS,
        names::JOB_DURATION_SECONDS,
        names::JOBS_COMPLETED,
        names::JOBS_FAILED,
        names::JOBS_RETRIED,
        names::JOBS_DISCARDED,
        names::JOBS_INTERRUPTED,
        names::JOBS_UNKNOWN_TASK,
        names::WORKER_HEARTBEAT_AGE_SECONDS,
        names::CLAIM_DURATION_SECONDS,
        names::MAINTENANCE_JOBS,
        names::WAKEUP_HEALTHY,
        names::WAKEUPS_RECEIVED,
        names::BREAKER_STATE,
        names::THROTTLE_HEADROOM,
    ];

    for n in all {
        assert!(n.starts_with("stampeded."), "{n} is not namespaced");
        assert_eq!(n, n.to_lowercase(), "{n} should be lowercase");
        assert!(!n.contains(' '), "{n} has whitespace");
    }

    let mut sorted = all.to_vec();
    sorted.sort_unstable();
    let before = sorted.len();
    sorted.dedup();
    assert_eq!(sorted.len(), before, "two metrics share a name");
}

#[test]
fn durations_say_so_in_their_name() {
    // Prometheus rejects one name as both counter and histogram.
    for n in [
        names::JOB_DURATION_SECONDS,
        names::JOB_QUEUE_TIME_SECONDS,
        names::CLAIM_DURATION_SECONDS,
    ] {
        assert!(
            n.ends_with("_seconds"),
            "{n} is a duration and should say so"
        );
    }
    for n in [
        names::JOBS_COMPLETED,
        names::JOBS_FAILED,
        names::JOBS_INTERRUPTED,
    ] {
        assert!(!n.ends_with("_seconds"), "{n} is a count");
    }
}
