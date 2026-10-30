//! The queue planner: which queues a worker should claim from, and in what order.
//!
//! The daemon writes each verdict to `stampeded_queue_policies`; workers read it at claim.
//! The circuit needs pure leaves: conditions read a snapshot, verdict leaves always succeed.

use behavior_machines::{Node, Status};
use behavior_machines_circuit::{LowerError, Lowered, Schedule, State, TickError};

/// What a worker should do with a queue this round.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Verdict {
    /// Take its turn.
    Steady,
    /// Claim from it before the steady queues.
    Rush,
    /// Do not claim from it.
    Hold,
}

impl Verdict {
    /// The value stored in `stampeded_queue_policies.verdict`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Rush => "rush",
            Self::Hold => "hold",
        }
    }

    /// The stored value back; anything unrecognised reads as steady.
    pub fn parse(stored: &str) -> Self {
        match stored {
            "rush" => Self::Rush,
            "hold" => Self::Hold,
            _ => Self::Steady,
        }
    }
}

/// One queue's policy and the facts the planner judges it on.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct QueueSnapshot {
    pub queue: String,
    /// Concurrency cap across the fleet: claimed plus executing.
    pub max_running: Option<i32>,
    /// How long the next job in line may wait before the queue is rushed.
    pub max_wait_secs: Option<i32>,
    /// Claimed plus executing, from the counter table.
    pub running: i64,
    /// How long the head of the claim order has been due; `None` when nothing is claimable.
    pub head_wait_secs: Option<f64>,
    /// Paused by an operator; `stampeded_claim` refuses it whatever the verdict.
    pub paused: bool,
}

/// A verdict and, unless the queue is steady, why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub verdict: Verdict,
    pub reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Leaf {
    Paused,
    AtCapacity,
    Overdue,
    Verdict(Verdict),
}

impl Leaf {
    fn status(self, snapshot: &QueueSnapshot) -> Status {
        let holds = match self {
            Self::Paused => snapshot.paused,
            Self::AtCapacity => snapshot
                .max_running
                .is_some_and(|max| snapshot.running >= i64::from(max)),
            Self::Overdue => matches!(
                (snapshot.max_wait_secs, snapshot.head_wait_secs),
                (Some(max), Some(waited)) if waited > f64::from(max)
            ),
            Self::Verdict(_) => true,
        };
        if holds {
            Status::Success
        } else {
            Status::Failure
        }
    }
}

/// The policy. Order is precedence: a paused queue is held however late its jobs
/// are, and a queue both full and behind is held.
///
/// Every composite is reactive, so a verdict depends on the snapshot alone.
fn policy() -> Node<Leaf> {
    Node::reactive_fallback([
        Node::reactive_sequence([
            Node::leaf(Leaf::Paused),
            Node::leaf(Leaf::Verdict(Verdict::Hold)),
        ]),
        Node::reactive_sequence([
            Node::leaf(Leaf::AtCapacity),
            Node::leaf(Leaf::Verdict(Verdict::Hold)),
        ]),
        Node::reactive_sequence([
            Node::leaf(Leaf::Overdue),
            Node::leaf(Leaf::Verdict(Verdict::Rush)),
        ]),
        Node::leaf(Leaf::Verdict(Verdict::Steady)),
    ])
}

/// The lowered policy, built once and shared by every queue.
#[derive(Debug)]
pub struct Planner {
    lowered: Lowered,
    /// Per leaf slot: the leaf and its node index.
    leaves: Vec<(usize, Leaf)>,
}

impl Planner {
    pub fn new() -> Result<Self, LowerError> {
        // `map` visits leaves in pre-order, the circuit's slot order.
        let mut collected = Vec::new();
        let numbered = policy().map(|leaf| collected.push(leaf));
        let lowered = Lowered::new(&numbered)?;

        let plan = lowered.plan();
        let leaves = collected
            .into_iter()
            .enumerate()
            .filter_map(|(slot, leaf)| plan.leaf_node(slot).map(|node| (node, leaf)))
            .collect();
        Ok(Self { lowered, leaves })
    }

    /// The verdict for one queue.
    pub fn decide(&self, snapshot: &QueueSnapshot) -> Decision {
        let schedule = match self.evaluate(snapshot) {
            Ok(schedule) => schedule,
            // Unreachable for a tree lowered by `new`.
            Err(e) => {
                return Decision {
                    verdict: Verdict::Steady,
                    reason: Some(format!("planner could not evaluate the policy: {e}")),
                };
            }
        };

        let verdict = self
            .leaves
            .iter()
            .find_map(|(node, leaf)| match leaf {
                Leaf::Verdict(verdict) if schedule.reached(*node) => Some(*verdict),
                _ => None,
            })
            .unwrap_or(Verdict::Steady);

        let reason = match verdict {
            Verdict::Steady => None,
            Verdict::Hold if snapshot.paused => Some("paused".to_owned()),
            Verdict::Hold => Some(format!(
                "{} running, max {}",
                snapshot.running,
                snapshot.max_running.unwrap_or_default()
            )),
            Verdict::Rush => Some(format!(
                "next job waited {:.0}s, max {}s",
                snapshot.head_wait_secs.unwrap_or_default(),
                snapshot.max_wait_secs.unwrap_or_default()
            )),
        };
        Decision { verdict, reason }
    }

    fn evaluate(&self, snapshot: &QueueSnapshot) -> Result<Schedule, TickError> {
        let statuses: Vec<Status> = self
            .leaves
            .iter()
            .map(|(_, leaf)| leaf.status(snapshot))
            .collect();
        // Fresh per queue: a reactive tree carries nothing.
        let mut state = State::initial(&self.lowered);
        self.lowered.evaluate(&mut state, &statuses)
    }
}

#[cfg(test)]
mod tests;
