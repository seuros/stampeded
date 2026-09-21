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
pub mod dashboard;

#[cfg(feature = "game")]
pub mod game;

pub use api::{ApiState, router};

use tokio::sync::watch;
use tracing::info;

/// Serve the API on `addr` until `shutdown` flips to true.
///
/// Separate from `router()` so the router can be mounted alongside the
/// dashboard and MCP transport in one rama server — sharing a single HTTP stack
/// is the reason rama is here at all (plans/14).
///
/// The layer stack is not decoration: `Router` is neither `Clone` nor
/// infallible, so `ArcLayer` makes it shareable across connections and
/// `ErrorHandlerLayer` turns its `RouterError` into a response. Without both,
/// this does not compile.
///
/// The shutdown wiring is not optional either. `TcpListener::serve` stops
/// accepting when its `Executor` carries a graceful guard — and does NOT when
/// the executor is `Executor::default()`, which has none. Built that way, this
/// future never resolves, and a daemon awaiting its tasks hangs on SIGTERM
/// forever instead of exiting. That was a real bug, found by sending the signal.
pub async fn serve(
    addr: &str,
    state: ApiState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    use rama::Layer;
    use rama::graceful::Shutdown;
    use rama::http::layer::error_handling::ErrorHandlerLayer;
    use rama::http::server::HttpServer;
    use rama::layer::ArcLayer;
    use rama::rt::Executor;
    use rama::tcp::server::TcpListener;

    let app_router = {
        let r = router(state.clone())
            .with_get("/dashboard", dashboard::index)
            .with_get("/dashboard/stream", dashboard::stream)
            // Datastar served from the binary, not a CDN. See dashboard::script.
            .with_get("/dashboard/datastar.js", dashboard::script)
            .with_get("/dashboard/datastar.js.map", dashboard::script_map);

        #[cfg(feature = "game")]
        {
            r.with_get("/game", game::index)
                .with_get("/game/stream", game::stream)
                .with_get("/game/three.module.js", game::three_module)
                .with_get("/game/loaders/gltf-loader.js", game::gltf_loader)
                .with_get("/game/utils/BufferGeometryUtils.js", game::buffer_geometry_utils)
                .with_get("/game/utils/SkeletonUtils.js", game::skeleton_utils)
                .with_get("/game/models/{name}", |rama::http::service::web::extract::Path(name): rama::http::service::web::extract::Path<String>| async move {
                    game::model(name).await
                })
        }

        #[cfg(not(feature = "game"))]
        {
            r
        }
    };

    let app = (ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(app_router);
    // The guard is what makes `serve` return: it cancels when the signal
    // future resolves, which here is our own shutdown channel rather than a
    // SIGTERM handler, so the daemon keeps owning signal policy.
    let graceful = Shutdown::new(async move {
        let _ = shutdown.changed().await;
    });
    let exec = Executor::graceful(graceful.guard());
    let service = HttpServer::auto(exec.clone()).service(app);

    info!(%addr, "http api listening");
    TcpListener::bind_address(addr, exec)
        .await
        .map_err(|e| format!("bind {addr}: {e}"))?
        .serve(service)
        .await;

    // Accepting has stopped; let in-flight requests finish. An SSE stream can
    // be open indefinitely, so this is bounded rather than unbounded — a job
    // watcher must not keep the process alive.
    let waited = graceful
        .shutdown_with_limit(std::time::Duration::from_secs(10))
        .await;
    info!(?waited, "http api stopped");
    Ok(())
}
