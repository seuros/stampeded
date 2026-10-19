//! A Rust worker for a job enqueued from Rails:
//!
//!   class ParseInvoiceJob < ApplicationJob
//!     queue_as :parsing
//!     stampeded task: "invoice.parse"
//!   end
//!
//!   ParseInvoiceJob.perform_later(document_id: "inv-42")
//!
//! It answers with `invoice.store`, a Ruby job that takes `args` as keywords.
//!
//!   STAMPEDED_DATABASE_URL=postgres://localhost/stampeded_demo \
//!     cargo run -p stampeded --example cross_runtime

use serde::Deserialize;
use stampeded::{NewJob, Queue, Worker, WorkerConfig};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Deserialize)]
struct ParseInvoice {
    document_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("STAMPEDED_DATABASE_URL").map_err(|_| "set STAMPEDED_DATABASE_URL")?;
    let run_for = std::env::var("RUN_FOR_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs);

    let queue = Queue::new(sqlx::PgPool::connect(&url).await?);
    let results = queue.clone();

    let worker = Worker::new(
        queue,
        WorkerConfig {
            queues: stampeded::Queues::one("parsing"),
            ..Default::default()
        },
    )
    .register("invoice.parse", move |job| {
        let results = results.clone();
        async move {
            let invoice: ParseInvoice = serde_json::from_value(job.args.clone())?;
            let lines = invoice.document_id.len();
            println!("parsed {} in Rust: {lines} lines", invoice.document_id);

            results
                .enqueue(
                    NewJob::new("default", "invoice.store").args(serde_json::json!({
                        "document_id": invoice.document_id,
                        "lines": lines,
                    })),
                )
                .await?;
            Ok(())
        }
    });

    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        match run_for {
            Some(d) => tokio::time::sleep(d).await,
            None => {
                if let Err(e) = tokio::signal::ctrl_c().await {
                    eprintln!("cannot listen for ctrl-c ({e}); stopping now");
                }
            }
        }
        // Err only once the worker has already stopped.
        let _ = tx.send(true);
    });

    worker.run(rx).await?;
    Ok(())
}
