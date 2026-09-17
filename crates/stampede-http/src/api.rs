//! The HTTP API.
//!
//! Deliberately small. Enqueue, inspect, and queue depth — the operations a
//! caller outside the trust boundary needs. Claiming is NOT exposed: it carries
//! the fence, the attempt accounting and the ownership rules, and a second
//! implementation of those over HTTP is exactly the drift the SQL-function
//! design exists to prevent (plans/05).

use rama::http::StatusCode;
use rama::http::service::web::Router;
use rama::http::service::web::extract::{Path, State};
use rama::http::service::web::extract::body::Json as JsonBody;
use rama::http::service::web::response::{IntoResponse, Json};
use serde::{Deserialize, Serialize};
use stampede_core::{NewJob, Queue};
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub struct ApiState {
    pub queue: Arc<Queue>,
}

impl ApiState {
    pub fn new(queue: Queue) -> Self {
        Self { queue: Arc::new(queue) }
    }
}

#[derive(Debug, Deserialize)]
pub struct EnqueueRequest {
    pub queue: String,
    pub task: String,
    #[serde(default)]
    pub args: serde_json::Value,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub priority: Option<i16>,
    #[serde(default)]
    pub max_attempts: Option<i16>,
    /// When set, enables per-queue deduplication on this scope.
    #[serde(default)]
    pub unique: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct EnqueueResponse {
    pub id: Option<uuid::Uuid>,
    /// False when a deduplicated job was already queued. Deliberately not an
    /// error: the caller's intent ("ensure this job exists") was satisfied.
    pub created: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Build the router.
pub fn router(state: ApiState) -> Router<ApiState> {
    Router::new_with_state(state)
        .with_get("/health", health)
        .with_post("/jobs", enqueue)
        .with_get("/queues/{queue}/depth", depth)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn enqueue(
    State(state): State<ApiState>,
    JsonBody(body): JsonBody<EnqueueRequest>,
) -> impl IntoResponse {
    let mut job = NewJob::new(body.queue, body.task).args(body.args);
    if let Some(t) = body.tenant_id {
        job = job.tenant(t);
    }
    if let Some(p) = body.priority {
        job = job.priority(p);
    }
    if let Some(m) = body.max_attempts {
        job = job.max_attempts(m);
    }
    if let Some(u) = body.unique {
        job = job.unique(u);
    }

    match state.queue.enqueue(job).await {
        // 201 with created:false for a deduplicated job, not 409: the caller
        // asked us to ensure the job exists, and it does. Making it an error
        // forces every caller to distinguish "already queued" from "failed".
        Ok(id) => (
            StatusCode::CREATED,
            Json(EnqueueResponse { created: id.is_some(), id }),
        )
            .into_response(),
        Err(e) => {
            warn!(error = %e, "enqueue failed");
            internal("enqueue failed")
        }
    }
}

async fn depth(
    State(state): State<ApiState>,
    Path(queue): Path<String>,
) -> impl IntoResponse {
    match state.queue.depth(&queue).await {
        Ok(rows) => {
            let map: serde_json::Map<String, serde_json::Value> = rows
                .into_iter()
                .map(|(s, n)| (s, serde_json::Value::from(n)))
                .collect();
            Json(serde_json::json!({ "queue": queue, "depth": map })).into_response()
        }
        Err(e) => {
            warn!(error = %e, "depth query failed");
            internal("depth query failed")
        }
    }
}

fn internal(msg: &str) -> rama::http::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: msg.to_string() }),
    )
        .into_response()
}

#[cfg(test)]
mod tests;
