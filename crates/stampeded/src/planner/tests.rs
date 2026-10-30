use super::*;

fn snapshot(
    max_running: Option<i32>,
    running: i64,
    max_wait: Option<i32>,
    waited: Option<f64>,
) -> QueueSnapshot {
    QueueSnapshot {
        queue: "q".into(),
        max_running,
        max_wait_secs: max_wait,
        running,
        head_wait_secs: waited,
        paused: false,
    }
}

fn paused(snapshot: QueueSnapshot) -> QueueSnapshot {
    QueueSnapshot {
        paused: true,
        ..snapshot
    }
}

#[test]
fn verdicts_follow_the_policy() {
    let planner = Planner::new().expect("the policy lowers");

    let cases = [
        (
            "no policy at all",
            snapshot(None, 9, None, Some(9_999.0)),
            Verdict::Steady,
        ),
        (
            "under the cap",
            snapshot(Some(2), 1, None, None),
            Verdict::Steady,
        ),
        (
            "at the cap",
            snapshot(Some(1), 1, None, None),
            Verdict::Hold,
        ),
        (
            "over the cap",
            snapshot(Some(1), 3, None, None),
            Verdict::Hold,
        ),
        (
            "inside the wait target",
            snapshot(None, 0, Some(60), Some(59.0)),
            Verdict::Steady,
        ),
        (
            "past the wait target",
            snapshot(None, 0, Some(60), Some(61.0)),
            Verdict::Rush,
        ),
        (
            "nothing claimable",
            snapshot(None, 0, Some(60), None),
            Verdict::Steady,
        ),
        (
            "full and behind: the cap wins",
            snapshot(Some(1), 1, Some(60), Some(600.0)),
            Verdict::Hold,
        ),
        (
            "room and behind",
            snapshot(Some(2), 1, Some(60), Some(600.0)),
            Verdict::Rush,
        ),
        (
            "paused, however far behind",
            paused(snapshot(Some(2), 0, Some(60), Some(600.0))),
            Verdict::Hold,
        ),
        (
            "paused with no limits",
            paused(snapshot(None, 0, None, None)),
            Verdict::Hold,
        ),
    ];

    for (name, snapshot, expected) in cases {
        assert_eq!(planner.decide(&snapshot).verdict, expected, "{name}");
    }
}

#[test]
fn a_verdict_other_than_steady_says_why() {
    let planner = Planner::new().expect("the policy lowers");

    let hold = planner.decide(&snapshot(Some(1), 1, None, None));
    assert_eq!(hold.reason.as_deref(), Some("1 running, max 1"));

    let rush = planner.decide(&snapshot(None, 0, Some(60), Some(90.4)));
    assert_eq!(rush.reason.as_deref(), Some("next job waited 90s, max 60s"));

    // Also at its cap, but the pause is what an operator must undo.
    let pause = planner.decide(&paused(snapshot(Some(1), 1, None, None)));
    assert_eq!(pause.reason.as_deref(), Some("paused"));

    assert_eq!(planner.decide(&snapshot(None, 0, None, None)).reason, None);
}
