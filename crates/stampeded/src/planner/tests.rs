use super::*;
use behavior_machines::{NodeId, Observer, Time, Tree};

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

    assert_eq!(planner.decide(&snapshot(None, 0, None, None)).reason, None);
}

// REAL: the behavior-machines engine walking the same tree, for parity.
struct Fixed(Status);

impl behavior_machines::Leaf<()> for Fixed {
    fn tick(&self, _ctx: &mut (), _now: Time) -> Status {
        self.0
    }
}

#[derive(Default)]
struct Reached(Vec<usize>);

impl Observer for Reached {
    fn ticked(&mut self, node: NodeId, _status: Status) {
        self.0.push(node.index());
    }
}

#[test]
fn the_circuit_reaches_exactly_what_the_engine_walk_reaches() {
    // The circuit must agree with the engine for every combination of conditions.
    let planner = Planner::new().expect("the policy lowers");

    for at_capacity in [false, true] {
        for overdue in [false, true] {
            let snap = snapshot(
                Some(1),
                i64::from(at_capacity),
                Some(60),
                Some(if overdue { 61.0 } else { 1.0 }),
            );

            let schedule = planner.evaluate(&snap).expect("evaluates");
            let mut circuit: Vec<usize> = (0..schedule.status.len())
                .filter(|n| schedule.reached(*n))
                .collect();
            circuit.sort_unstable();

            let mut tree =
                Tree::new(policy().map(|leaf| Fixed(leaf.status(&snap)))).expect("builds");
            let mut walk = Reached::default();
            let root = tree.tick_observed(&mut (), Time::ZERO, &mut walk);
            walk.0.sort_unstable();

            assert_eq!(
                circuit, walk.0,
                "at_capacity={at_capacity} overdue={overdue}"
            );
            assert_eq!(
                schedule.root(),
                Some(root),
                "at_capacity={at_capacity} overdue={overdue}"
            );
        }
    }
}
