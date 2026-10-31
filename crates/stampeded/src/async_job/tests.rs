//! The worker path is in tests/typed_jobs.rs.

use super::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Extract {
    name: String,
    version: String,
}

impl AsyncJob for Extract {
    const TASK: &'static str = "crate.extract";
    const QUEUE: &'static str = "vein";
    type Output = usize;

    fn run(&self, _ctx: &JobContext) -> impl Future<Output = JobResult<usize>> + Send {
        std::future::ready(Ok(self.name.len()))
    }
}

#[test]
fn new_job_carries_the_task_the_queue_and_the_payload_as_args() {
    let job = Extract {
        name: "serde".into(),
        version: "1.0.228".into(),
    };
    let row = job.new_job().unwrap();
    assert_eq!(row.task, "crate.extract");
    assert_eq!(row.queue, "vein");
    assert_eq!(
        row.args,
        serde_json::json!({"name": "serde", "version": "1.0.228"})
    );
    assert_eq!(row.max_attempts, NewJob::new("x", "y").max_attempts);
    assert!(row.dedup_scope.is_none());
}

#[test]
fn the_payload_comes_back_from_args_as_the_same_value() {
    let job = Extract {
        name: "serde".into(),
        version: "1.0.228".into(),
    };
    let row = job.new_job().unwrap();
    let back: Extract = serde_json::from_value(row.args).unwrap();
    assert_eq!(back, job);
}

#[test]
fn a_payload_that_is_not_an_object_is_refused_at_enqueue() {
    #[derive(Serialize, Deserialize)]
    struct Ping;

    impl AsyncJob for Ping {
        const TASK: &'static str = "ping";
        type Output = ();

        fn run(&self, _ctx: &JobContext) -> impl Future<Output = JobResult<()>> + Send {
            std::future::ready(Ok(()))
        }
    }

    let err = Ping.new_job().unwrap_err();
    assert!(matches!(err, Error::Serde(_)), "{err}");
    assert!(err.to_string().contains("not null"), "{err}");
    assert_eq!(Ping::QUEUE, "default");
}

#[test]
fn a_handler_returning_the_crates_own_error_keeps_its_classification() {
    let serde = serde_json::from_str::<Extract>("{}").unwrap_err();
    let err = Error::from_handler(Box::new(Error::Serde(serde)));
    assert!(err.is_unrecoverable(), "{err}");

    let err = Error::from_handler(Box::new(Error::Timeout(std::time::Duration::from_secs(1))));
    assert!(matches!(err, Error::Timeout(_)), "{err}");

    let err = Error::from_handler(Box::new(crate::RetryIn::secs(5)));
    let Error::Handler(src) = &err else {
        panic!("expected Handler, got {err}");
    };
    assert!(src.downcast_ref::<crate::RetryIn>().is_some());
}
