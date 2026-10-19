//! Adaptive poll decisions; the listener is covered by the integration tests.

use super::*;

#[test]
fn a_full_batch_means_go_again_immediately() {
    // Otherwise a backlog drains at one batch per poll interval.
    assert_eq!(NextPoll::after_claim(5, 5, 10), NextPoll::Immediate);
    assert_eq!(
        NextPoll::Immediate.delay(Duration::from_secs(1)),
        Duration::ZERO
    );
}

#[test]
fn a_partial_batch_means_the_queue_is_drained() {
    // Less than asked for means nothing is left.
    assert_eq!(NextPoll::after_claim(2, 5, 10), NextPoll::Idle);
    assert_eq!(NextPoll::after_claim(0, 5, 10), NextPoll::Idle);
    assert_eq!(
        NextPoll::Idle.delay(Duration::from_secs(1)),
        Duration::from_secs(1)
    );
}

#[test]
fn saturation_wins_over_a_full_batch() {
    // Claiming with no free slots would park jobs behind a semaphore, looking like running work.
    assert_eq!(NextPoll::after_claim(5, 5, 0), NextPoll::Saturated);
}

#[test]
fn the_saturated_sleep_is_long_but_bounded() {
    let d = NextPoll::Saturated.delay(Duration::from_secs(1));
    assert!(d > Duration::from_secs(1));
    assert!(
        d <= Duration::from_secs(30),
        "an unbounded sleep here would be a stall"
    );
}

#[tokio::test]
async fn a_disabled_wakeup_never_fires() {
    let w = Wakeup::disabled();
    assert!(!w.is_healthy());

    let mut rx = w.subscribe("default");
    let fired = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
    assert!(fired.is_err(), "a disabled wakeup must not produce wakeups");
}

#[tokio::test]
async fn wakeups_are_filtered_to_the_subscribed_queue() {
    // The daemon's one listener fans out every queue's notifications to every worker.
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
