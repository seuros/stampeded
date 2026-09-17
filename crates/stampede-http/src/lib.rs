//! HTTP edge for clients outside the trust boundary.
//!
//! Most callers should enqueue with a plain `INSERT` from their own Postgres
//! driver — that is the whole polyglot argument (plans/05) and it gets
//! transactional enqueue for free. This exists for callers that cannot or
//! should not hold a Postgres connection: browsers, edge functions, third
//! parties, anything across a trust boundary.
//!
//! Built on rama so the API, the dashboard and the MCP transport share one HTTP
//! stack (plans/14).

pub mod api;

pub use api::{ApiState, router};

use stampede_core::Queue;
use tracing::info;

/// Serve the API on `addr`.
///
/// Separate from `router()` so the router can be mounted alongside the
/// dashboard and MCP transport in one rama server — sharing a single HTTP stack
/// is the reason rama is here at all (plans/14).
///
/// The layer stack is not decoration: `Router` is neither `Clone` nor
/// infallible, so `ArcLayer` makes it shareable across connections and
/// `ErrorHandlerLayer` turns its `RouterError` into a response. Without both,
/// this does not compile.
pub async fn serve(addr: &str, queue: Queue) -> Result<(), Box<dyn std::error::Error>> {
    use rama::Layer;
    use rama::http::layer::error_handling::ErrorHandlerLayer;
    use rama::http::server::HttpServer;
    use rama::layer::ArcLayer;
    use rama::rt::Executor;
    use rama::tcp::server::TcpListener;

    let app = (ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(router(ApiState::new(queue)));
    let exec = Executor::default();
    let service = HttpServer::auto(exec.clone()).service(app);

    info!(%addr, "http api listening");
    TcpListener::bind_address(addr, exec)
        .await
        .map_err(|e| format!("bind {addr}: {e}"))?
        .serve(service)
        .await;
    Ok(())
}
