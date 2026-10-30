//! HTTP edge for clients that cannot hold a Postgres connection.
//!
//! Everyone else should enqueue with a plain `INSERT`, which is transactional.

pub mod api;
pub mod dashboard;
pub mod trends;

#[cfg(feature = "game")]
pub mod game;

#[cfg(test)]
mod test_support;

pub use api::{ApiState, router};

use tokio::sync::watch;
use tracing::{info, warn};

/// Serve the API and dashboard on `addr` until `shutdown` flips to true.
pub async fn serve(
    addr: &str,
    state: ApiState,
    mut shutdown: watch::Receiver<bool>,
) -> crate::Result<()> {
    use api::DashboardToken;
    use rama::Layer;
    use rama::graceful::Shutdown;
    use rama::http::layer::error_handling::ErrorHandlerLayer;
    #[cfg(feature = "game")]
    use rama::http::service::web::extract::Path;
    use rama::http::service::web::router::DefaultEndpointLayer;
    use rama::layer::ArcLayer;
    use rama::rt::Executor;
    use rama::tcp::server::TcpListener;
    use rama_http_backend::server::HttpServer;

    let app_router = {
        // Static assets are public; the pages that load them are not.
        let r = router(state.clone())
            // Datastar served from the binary, not a CDN.
            .with_get("/dashboard/datastar.js", dashboard::script)
            .with_get("/dashboard/datastar.js.map", dashboard::script_map);

        #[cfg(feature = "game")]
        let r = r
            .with_get("/game/three.module.min.js", game::three_module)
            .with_get("/game/three.core.min.js", game::three_core)
            .with_get("/game/loaders/gltf-loader.js", game::gltf_loader)
            .with_get(
                "/game/utils/BufferGeometryUtils.js",
                game::buffer_geometry_utils,
            )
            .with_get("/game/utils/SkeletonUtils.js", game::skeleton_utils)
            .with_get("/game/models/Textures/colormap.png", game::model_texture)
            .with_get(
                "/game/models/{name}",
                |Path(name): Path<String>| async move { game::model(&name) },
            );

        // Every route from here on shows job data, so answers only with the token.
        let r = r.with_endpoint_layer((
            DashboardToken::layer(state.clone()),
            DefaultEndpointLayer::default(),
        ));

        #[cfg(feature = "game")]
        let r = r
            .with_get("/game", game::index)
            .with_get("/game/stream", game::stream);

        r.with_get("/dashboard", dashboard::index)
            .with_get("/dashboard/stream", dashboard::stream)
    };

    // Router is neither Clone nor infallible; ArcLayer and ErrorHandlerLayer fix both.
    let app = (ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(app_router);
    let sampler_shutdown = shutdown.clone();
    // Without a graceful guard `serve` never returns and SIGTERM hangs the daemon.
    let graceful = Shutdown::new(async move {
        // Err means the sender is gone, which is a shutdown too.
        let _ = shutdown.changed().await;
    });
    let exec = Executor::graceful(graceful.guard());
    let service = HttpServer::auto(exec.clone()).service(app);

    let listener = TcpListener::bind_address(addr, exec)
        .await
        .map_err(|source| crate::Error::Bind {
            addr: addr.to_owned(),
            source,
        })?;

    // Sample while the dashboard is served, so history exists before anyone looks.
    let sampler = state.dashboard_token.is_some().then(|| {
        tokio::spawn(trends::run(
            state.queue.clone(),
            state.trends.clone(),
            sampler_shutdown,
        ))
    });

    info!(%addr, "http api listening");
    listener.serve(service).await;

    // Bounded: an open SSE stream must not keep the process alive.
    let waited = graceful
        .shutdown_with_limit(std::time::Duration::from_secs(10))
        .await;
    info!(?waited, "http api stopped");

    // Awaited so a panicked sampler is reported, not silently frozen.
    if let Some(sampler) = sampler
        && let Err(e) = sampler.await
    {
        warn!(error = %e, "trend sampler died");
    }
    Ok(())
}
