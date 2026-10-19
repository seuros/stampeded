use super::*;

fn reading(completed: i64, waiting: i64, oldest_secs: Option<f64>) -> BTreeMap<String, Reading> {
    BTreeMap::from([(
        "mail".to_string(),
        Reading {
            completed,
            waiting,
            oldest_secs,
        },
    )])
}

fn points(trends: &Trends, queue: &str) -> Vec<Point> {
    trends
        .snapshot()
        .into_iter()
        .find(|(q, _)| q == queue)
        .map(|(_, p)| p)
        .unwrap_or_default()
}

#[test]
fn rate_is_completed_jobs_per_second_between_samples() {
    let trends = Trends::default();
    let t0 = Instant::now();
    trends.record(t0, &reading(100, 40, Some(12.0)));
    trends.record(t0 + Duration::from_secs(5), &reading(150, 30, Some(8.0)));

    let p = points(&trends, "mail");
    assert_eq!(p.len(), 2);
    assert_eq!(p[0].rate, None, "a first sample has no interval to measure");
    assert_eq!(p[1].rate, Some(10.0));
    assert_eq!((p[1].waiting, p[1].oldest_secs), (30, Some(8.0)));
}

#[test]
fn a_prune_carries_the_last_rate_instead_of_drawing_zero() {
    // The pruner can make the completed count fall; a zero would read as an outage.
    let trends = Trends::default();
    let t0 = Instant::now();
    trends.record(t0, &reading(100, 0, None));
    trends.record(t0 + Duration::from_secs(5), &reading(150, 0, None));
    trends.record(t0 + Duration::from_secs(10), &reading(20, 0, None));

    assert_eq!(points(&trends, "mail")[2].rate, Some(10.0));
}

#[test]
fn history_older_than_the_window_is_dropped() {
    let trends = Trends::default();
    let t0 = Instant::now();
    trends.record(t0, &reading(0, 1, None));
    trends.record(t0 + WINDOW + Duration::from_secs(1), &reading(5, 1, None));

    let p = points(&trends, "mail");
    assert_eq!(p.len(), 1, "the sample older than the window must go");
}

#[test]
fn a_queue_that_stops_reporting_ages_out_and_disappears() {
    let trends = Trends::default();
    let t0 = Instant::now();
    trends.record(t0, &reading(1, 1, None));
    let other = BTreeMap::from([("webhooks".to_string(), Reading::default())]);
    trends.record(t0 + WINDOW + Duration::from_secs(1), &other);

    let queues: Vec<String> = trends.snapshot().into_iter().map(|(q, _)| q).collect();
    assert_eq!(queues, ["webhooks"]);
}
