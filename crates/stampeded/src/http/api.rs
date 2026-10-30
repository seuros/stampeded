//! The HTTP API: enqueue, inspect and queue depth.
//!
//! Claiming is not exposed; its rules live only in the SQL functions.

use crate::{NewJob, Period, Queue};
use rama::futures::stream;
use rama::http::header;
use rama::http::layer::validate_request::{ValidateRequest, ValidateRequestHeaderLayer};
use rama::http::service::web::Router;
use rama::http::service::web::extract::body::Json as JsonBody;
use rama::http::service::web::extract::{Path, Query, State};
use rama::http::service::web::response::{IntoResponse, Json, Sse};
use rama::http::sse::Event;
use rama::http::sse::server::KeepAlive;
use rama::http::{Body, Request, Response, StatusCode};
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
    /// Throughput, depth and backlog-age history. See [`crate::http::trends`].
    pub trends: Arc<crate::http::trends::Trends>,
}

impl ApiState {
    pub fn new(queue: Queue) -> Self {
        Self {
            queue: Arc::new(queue),
            panels: Arc::new(PanelCache::default()),
            dashboard_token: None,
            wakeups: WakeupStatus::NotApplicable,
            trends: Arc::default(),
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
        token_matches(self.dashboard_token.as_deref(), presented)
    }
}

/// Constant-time, so the comparison does not time how much of a guess was right.
fn token_matches(expected: Option<&String>, presented: Option<&str>) -> bool {
    let (Some(expected), Some(given)) = (expected, presented) else {
        return false;
    };
    let (a, b) = (expected.as_bytes(), given.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Lets a request through only with the dashboard token as `?token=`. A layer, so a
/// page or stream cannot forget to ask; 404, not 401, so as not to reveal that a
/// dashboard exists.
#[derive(Clone)]
pub struct DashboardToken(Option<Arc<String>>);

impl DashboardToken {
    pub fn layer(state: &ApiState) -> ValidateRequestHeaderLayer<Self> {
        ValidateRequestHeaderLayer::custom(Self(state.dashboard_token.clone()))
    }
}

#[derive(Deserialize)]
struct Presented {
    token: Option<String>,
}

impl ValidateRequest<Body> for DashboardToken {
    type ResponseBody = Body;

    fn validate(
        &self,
        req: Request,
    ) -> impl Future<Output = Result<Request, Response>> + Send + '_ {
        let presented = Query::<Presented>::try_from_uri(req.uri())
            .ok()
            .and_then(|Query(p)| p.token);
        std::future::ready(if token_matches(self.0.as_deref(), presented.as_deref()) {
            Ok(req)
        } else {
            Err((StatusCode::NOT_FOUND, "not found").into_response())
        })
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
        return error(
            StatusCode::BAD_REQUEST,
            "max_attempts must be at least 1: the claim predicate is \
             `attempt < max_attempts`, so 0 would make the job \
             permanently unclaimable",
        );
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
                return error(
                    StatusCode::BAD_REQUEST,
                    format!("unique_period must be minute, hour, day, week or month, got {bad:?}"),
                );
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
        Err(e) => internal("enqueue failed", &e),
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
        Err(e) => internal("depth query failed", &e),
    }
}

/// One job's current state; the queue in the path prunes partitions.
async fn job_status(
    State(state): State<ApiState>,
    Path(JobPath { queue, id }): Path<JobPath>,
) -> impl IntoResponse {
    let id = match job_id(&id) {
        Ok(id) => id,
        Err(bad) => return bad.into_response(),
    };

    match state.queue.fetch(&queue, id).await {
        Ok(Some(job)) => Json(job_json(&job)).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            format!("no job {id} in queue {queue:?}"),
        ),
        Err(e) => internal("job lookup failed", &e),
    }
}

/// A job's state transitions as server-sent events, polled, ending at a terminal state.
async fn job_events(
    State(state): State<ApiState>,
    Path(JobPath { queue, id }): Path<JobPath>,
) -> impl IntoResponse {
    let id = match job_id(&id) {
        Ok(id) => id,
        Err(bad) => return bad.into_response(),
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

/// A per-job route's id that is not a UUID: the caller's error, a 400.
struct NotAUuid;

impl IntoResponse for NotAUuid {
    fn into_response(self) -> Response {
        error(StatusCode::BAD_REQUEST, "id is not a UUID")
    }
}

fn job_id(id: &str) -> Result<uuid::Uuid, NotAUuid> {
    uuid::Uuid::parse_str(id).map_err(|_| NotAUuid)
}

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(ErrorResponse { error: msg.into() })).into_response()
}

/// A 500 naming what failed; the cause goes to the log, not to the caller.
fn internal(what: &str, cause: &crate::Error) -> Response {
    warn!(error = %cause, "{what}");
    error(StatusCode::INTERNAL_SERVER_ERROR, what)
}

#[cfg(test)]
mod tests;
