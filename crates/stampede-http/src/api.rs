//! The HTTP API.
//!
//! Deliberately small. Enqueue, inspect, and queue depth — the operations a
//! caller outside the trust boundary needs. Claiming is NOT exposed: it carries
//! the fence, the attempt accounting and the ownership rules, and a second
//! implementation of those over HTTP is exactly the drift the SQL-function
//! design exists to prevent (plans/05).

use rama::futures::stream;
use rama::http::StatusCode;
use rama::http::header;
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use rama::http::service::web::Router;
use rama::http::service::web::extract::{Path, State};
use rama::http::service::web::extract::body::Json as JsonBody;
use rama::http::service::web::response::{IntoResponse, Json, Sse};
use serde::{Deserialize, Serialize};
use stampede_core::{NewJob, Period, Queue};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

#[derive(Clone)]
pub struct ApiState {
    pub queue: Arc<Queue>,
    /// Shared secret for the dashboard. `None` means the dashboard is NOT
    /// served at all.
    ///
    /// plans/06 asks for secure-by-default here and names the precedent: in
    /// this codebase's own history, GoodJob's dashboard sat unauthenticated on
    /// staging and production for a long time because auth was opt-in and
    /// nobody opted in. Opt-in auth is how that happens, so this is opt-OUT:
    /// no token, no dashboard.
    pub dashboard_token: Option<Arc<String>>,
    /// Whether the NOTIFY path is confirmed working, for the health panel.
    pub notifications_live: bool,
}

impl ApiState {
    pub fn new(queue: Queue) -> Self {
        Self {
            queue: Arc::new(queue),
            dashboard_token: None,
            notifications_live: false,
        }
    }

    /// Serve the dashboard, gated on this token.
    pub fn with_dashboard_token(mut self, token: impl Into<String>) -> Self {
        self.dashboard_token = Some(Arc::new(token.into()));
        self
    }

    pub fn with_notifications_live(mut self, live: bool) -> Self {
        self.notifications_live = live;
        self
    }

    /// Constant-time token comparison.
    ///
    /// Not paranoia theatre: a byte-at-a-time `==` on a secret compared against
    /// attacker-supplied input is the textbook timing oracle, and the fix is
    /// four lines.
    pub fn dashboard_authorised(&self, presented: Option<&str>) -> bool {
        let (Some(expected), Some(given)) = (&self.dashboard_token, presented) else {
            return false;
        };
        let (a, b) = (expected.as_bytes(), given.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
    }
}

/// Path parameters for the per-job routes.
///
/// A NAMED struct, not `Path<(String, String)>`: the tuple form binds by
/// parameter name order rather than by position in the path, so `{queue}` and
/// `{id}` arrive swapped and every lookup fails with "id is not a UUID". Found
/// by calling the endpoint, not by reading it.
#[derive(Debug, Deserialize)]
pub struct JobPath {
    pub queue: String,
    pub id: String,
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
    /// Bucket for `unique`: minute, hour, day, week or month. "One per hour"
    /// rather than "one ever".
    #[serde(default)]
    pub unique_period: Option<String>,
    /// What this job talks to. Keys the circuit breaker.
    #[serde(default)]
    pub destination: Option<String>,
    /// Where a terminal failure goes, with provenance.
    #[serde(default)]
    pub dead_letter_queue: Option<String>,
    /// Database-enforced wall-clock budget, in seconds.
    #[serde(default)]
    pub expire_seconds: Option<u64>,
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
        .with_get("/queues/{queue}/jobs/{id}", job_status)
        .with_get("/queues/{queue}/jobs/{id}/events", job_events)
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
        job = match body.unique_period.as_deref().map(parse_period).transpose() {
            Ok(Some(p)) => job.unique_per(u, p),
            Ok(None) => job.unique(u),
            Err(bad) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!(
                            "unique_period must be minute, hour, day, week or month, got {bad:?}"
                        ),
                    }),
                )
                    .into_response();
            }
        };
    }
    if let Some(d) = body.destination {
        job = job.destination(d);
    }
    if let Some(q) = body.dead_letter_queue {
        job = job.dead_letter(q);
    }
    if let Some(secs) = body.expire_seconds {
        job = job.expires_in(std::time::Duration::from_secs(secs));
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

/// One job's current state.
///
/// Pathed under its queue because jobs are partitioned by queue: without it
/// Postgres would scan every partition to find one row.
async fn job_status(
    State(state): State<ApiState>,
    Path(JobPath { queue, id }): Path<JobPath>,
) -> impl IntoResponse {
    let Ok(id) = uuid::Uuid::parse_str(&id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: "id is not a UUID".into() }),
        )
            .into_response();
    };

    match state.queue.fetch(&queue, id).await {
        Ok(Some(job)) => Json(job_json(&job)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: format!("no job {id} in queue {queue:?}") }),
        )
            .into_response(),
        Err(e) => {
            warn!(error = %e, "job lookup failed");
            internal("job lookup failed")
        }
    }
}

/// A job's state transitions, as server-sent events.
///
/// Polled rather than driven by LISTEN/NOTIFY, and only transitions are sent:
/// a caller waiting on one job wants to know when it MOVED, not that it is
/// still running. Putting per-job dashboards on the NOTIFY channel workers use
/// for wakeups would be the same mistake as streaming every row change
/// (plans/12).
///
/// The stream ends when the job reaches a terminal state, so a client can
/// simply read to EOF rather than deciding for itself when to stop.
async fn job_events(
    State(state): State<ApiState>,
    Path(JobPath { queue, id }): Path<JobPath>,
) -> impl IntoResponse {
    let Ok(id) = uuid::Uuid::parse_str(&id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: "id is not a UUID".into() }),
        )
            .into_response();
    };

    let stream = stream::unfold(
        (state, queue, id, None::<String>, false),
        move |(state, queue, id, last, done)| async move {
            if done {
                return None;
            }

            // Poll until the state differs from the last one emitted, so an
            // idle job costs one query per tick and no events at all.
            loop {
                match state.queue.fetch(&queue, id).await {
                    Ok(Some(job)) => {
                        let now = job.state.to_string();
                        if last.as_deref() != Some(now.as_str()) {
                            let terminal = job.finished_at.is_some();
                            let ev = Event::new()
                                .try_with_event("state")
                                .ok()?
                                .with_data(job_json(&job).to_string());
                            return Some((
                                Ok::<_, Infallible>(ev),
                                (state, queue, id, Some(now), terminal),
                            ));
                        }
                    }
                    // Gone (pruned, or never existed): end the stream rather
                    // than polling a row that will never appear.
                    Ok(None) => return None,
                    Err(e) => {
                        warn!(error = %e, "job event poll failed");
                        return None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        },
    );

    (
        [(header::CACHE_CONTROL, "no-cache")],
        Sse::new(stream).with_keep_alive(KeepAlive::new()),
    )
        .into_response()
}

fn job_json(job: &stampede_core::Job) -> serde_json::Value {
    serde_json::json!({
        "id": job.id,
        "queue": job.queue,
        "task": job.task,
        "state": job.state.to_string(),
        "attempt": job.attempt,
        "max_attempts": job.max_attempts,
        "args": job.args,
        "failure_kind": job.failure_kind,
        "last_error": job.last_error,
        "scheduled_at": job.scheduled_at,
        "finished_at": job.finished_at,
    })
}

fn parse_period(s: &str) -> Result<Period, String> {
    match s {
        "minute" => Ok(Period::Minute),
        "hour" => Ok(Period::Hour),
        "day" => Ok(Period::Day),
        "week" => Ok(Period::Week),
        "month" => Ok(Period::Month),
        other => Err(other.to_string()),
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
