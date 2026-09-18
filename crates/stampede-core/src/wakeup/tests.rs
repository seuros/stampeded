//! Adaptive poll decisions.
//!
//! The listener itself needs a live database and is covered by the integration
//! tests; this is the pure decision logic, which is where the latency bugs are.

use super::*;

#[test]
fn a_full_batch_means_go_again_immediately() {
    // The difference between draining a backlog at full speed and at one batch
    // per poll interval. Getting this wrong is invisible until the day there
    // is a backlog.
    assert_eq!(NextPoll::after_claim(5, 5, 10), NextPoll::Immediate);
    assert_eq!(
        NextPoll::Immediate.delay(Duration::from_secs(1)),
        Duration::ZERO
    );
}

#[test]
fn a_partial_batch_means_the_queue_is_drained() {
    // Claimed something, but less than asked for: there is nothing left, so
    // going again immediately would just be an empty query.
    assert_eq!(NextPoll::after_claim(2, 5, 10), NextPoll::Idle);
    assert_eq!(NextPoll::after_claim(0, 5, 10), NextPoll::Idle);
    assert_eq!(
        NextPoll::Idle.delay(Duration::from_secs(1)),
        Duration::from_secs(1)
    );
}

#[test]
fn saturation_wins_over_a_full_batch() {
    // No free slots means claiming more would park jobs in `executing` behind a
    // semaphore, where the reaper cannot tell them from running work.
    assert_eq!(NextPoll::after_claim(5, 5, 0), NextPoll::Saturated);
}

#[test]
fn the_saturated_sleep_is_long_but_bounded() {
    // Long because a wakeup or a finishing job normally interrupts it; bounded
    // because if both are somehow missed the worker still has to recover.
    let d = NextPoll::Saturated.delay(Duration::from_secs(1));
    assert!(d > Duration::from_secs(1));
    assert!(
        d <= Duration::from_secs(30),
        "an unbounded sleep here would be a stall"
    );
}

#[tokio::test]
async fn a_disabled_wakeup_never_fires() {
    // Poll-only must be a supported configuration, not a broken one.
    let w = Wakeup::disabled();
    assert!(!w.is_healthy());

    let mut rx = w.subscribe("default");
    let fired = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
    assert!(fired.is_err(), "a disabled wakeup must not produce wakeups");
}

#[tokio::test]
async fn wakeups_are_filtered_to_the_subscribed_queue() {
    // One listener per daemon fans out to every worker, so each worker sees
    // every queue's notifications and must ignore the ones that are not its
    // own -- otherwise a busy queue wakes idle workers on quiet ones.
    let w = Wakeup::disabled();
    let mut mail = w.subscribe("mail");

    w.tx.send("reports".into()).ok();
    let woke = tokio::time::timeout(Duration::from_millis(50), mail.recv()).await;
    assert!(
        woke.is_err(),
        "another queue's notification must not wake us"
    );

    w.tx.send("mail".into()).ok();
    let woke = tokio::time::timeout(Duration::from_millis(50), mail.recv()).await;
    assert!(woke.is_ok(), "our own queue must");
}
