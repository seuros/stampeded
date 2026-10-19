//! The HTTP API: enqueue, inspect and queue depth.
//!
//! Claiming is not exposed; its rules live only in the SQL functions.

use crate::{NewJob, Period, Queue};
use rama::futures::stream;
use rama::http::StatusCode;
use rama::http::header;
use rama::http::service::web::Router;
use rama::http::service::web::extract::body::Json as JsonBody;
use rama::http::service::web::extract::{Path, State};
use rama::http::service::web::response::{IntoResponse, Json, Sse};
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Rendered panels, shared across every connected dashboard client.
#[derive(Default)]
pub struct PanelCache {
    inner: std::sync::Mutex<PanelMap>,
}

type PanelMap = std::collections::HashMap<&'static str, (std::time::Instant, String)>;

impl PanelCache {
    /// Render `f`, or reuse a recent render of the same panel.
    pub async fn get<F>(&self, key: &'static str, ttl: std::time::Duration, f: F) -> Option<String>
    where
        F: std::future::Future<Output = Option<String>>,
    {
        {
            // Scoped: the std Mutex guard must drop before the await.
            let cache = self.lock();
            if let Some((at, html)) = cache.get(key)
                && at.elapsed() < ttl
            {
                return Some(html.clone());
            }
        }

        let html = f.await?;
        self.lock()
            .insert(key, (std::time::Instant::now(), html.clone()));
        Some(html)
    }

    /// Ignore poisoning: each entry is written whole by one `insert`.
    fn lock(&self) -> std::sync::MutexGuard<'_, PanelMap> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// What the dashboard should say about wakeups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeupStatus {
    /// Notifications confirmed round-tripping.
    Live,
    /// A listener runs but nothing came back, usually a pooler eating LISTEN.
    PollingOnly,
    /// No workers in this process, so no listener and nothing to wake.
    NotApplicable,
}

#[derive(Clone)]
pub struct ApiState {
    pub queue: Arc<Queue>,
    /// Shared render cache. See [`PanelCache`].
    pub panels: Arc<PanelCache>,
    /// Shared secret for the dashboard; `None` means it is not served at all.
    pub dashboard_token: Option<Arc<String>>,
    /// State of the NOTIFY path, for the health panel.
    pub wakeups: WakeupStatus,
}

impl ApiState {
    pub fn new(queue: Queue) -> Self {
        Self {
            queue: Arc::new(queue),
            panels: Arc::new(PanelCache::default()),
            dashboard_token: None,
            wakeups: WakeupStatus::NotApplicable,
        }
    }

    /// Serve the dashboard, gated on this token.
    #[must_use]
    pub fn with_dashboard_token(mut self, token: impl Into<String>) -> Self {
        self.dashboard_token = Some(Arc::new(token.into()));
        self
    }

    #[must_use]
    pub fn with_wakeups(mut self, status: WakeupStatus) -> Self {
        self.wakeups = status;
        self
    }

    /// Constant-time token comparison.
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
/// Named, not a tuple: the tuple form arrives with `{queue}` and `{id}` swapped.
#[derive(Debug, Deserialize)]
pub struct JobPath {
    pub queue: String,
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct EnqueueRequest {
    pub queue: String,
    pub task: String,
    /// Omitted or `null` means no arguments, stored as `{}`. See `NewJob::args`.
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
    /// Bucket for `unique`: minute, hour, day, week or month.
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
    /// False when a deduplicated job was already queued.
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
    // Also enforced by a CHECK; this gives the caller a readable reason.
    if let Some(m) = body.max_attempts
        && m < 1
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "max_attempts must be at least 1: the claim predicate is \
                        `attempt < max_attempts`, so 0 would make the job \
                        permanently unclaimable"
                    .into(),
            }),
        )
            .into_response();
    }

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
        // 201 with created:false for a duplicate, not 409: the job exists.
        Ok(id) => (
            StatusCode::CREATED,
            Json(EnqueueResponse {
                created: id.is_some(),
                id,
            }),
        )
            .into_response(),
        Err(e) => {
            warn!(error = %e, "enqueue failed");
            internal("enqueue failed")
        }
    }
}

async fn depth(State(state): State<ApiState>, Path(queue): Path<String>) -> impl IntoResponse {
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

/// One job's current state; the queue in the path prunes partitions.
async fn job_status(
    State(state): State<ApiState>,
    Path(JobPath { queue, id }): Path<JobPath>,
) -> impl IntoResponse {
    let Ok(id) = uuid::Uuid::parse_str(&id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "id is not a UUID".into(),
            }),
        )
            .into_response();
    };

    match state.queue.fetch(&queue, id).await {
        Ok(Some(job)) => Json(job_json(&job)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("no job {id} in queue {queue:?}"),
            }),
        )
            .into_response(),
        Err(e) => {
            warn!(error = %e, "job lookup failed");
            internal("job lookup failed")
        }
    }
}

/// A job's state transitions as server-sent events, polled, ending at a terminal state.
async fn job_events(
    State(state): State<ApiState>,
    Path(JobPath { queue, id }): Path<JobPath>,
) -> impl IntoResponse {
    let Ok(id) = uuid::Uuid::parse_str(&id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "id is not a UUID".into(),
            }),
        )
            .into_response();
    };

    let stream = stream::unfold(
        (state, queue, id, None::<String>, false),
        move |(state, queue, id, last, done)| async move {
            if done {
                return None;
            }

            // Emit only when the state changes.
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
                    // Pruned or never existed.
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

fn job_json(job: &crate::Job) -> serde_json::Value {
    serde_json::json!({
        "id": job.id,
        "queue": job.queue,
        "task": job.task,
        "state": job.state.to_string(),
        "attempt": job.attempt,
        "max_attempts": job.max_attempts,
        "args": job.args,
        "meta": job.meta,
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
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests;
