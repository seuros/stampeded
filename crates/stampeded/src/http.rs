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

/// The router behind the dashboard token: what [`serve_with`] hands the
/// embedding application to mount its own pages on.
pub type GatedRouter = rama::http::service::web::Router<
    ApiState,
    (
        rama::http::layer::validate_request::ValidateRequestHeaderLayer<api::DashboardToken>,
        rama::http::service::web::router::DefaultEndpointLayer,
    ),
>;

use tokio::sync::watch;
use tracing::{info, warn};

/// Serve the API and dashboard on `addr` until `shutdown` flips to true.
pub async fn serve(
    addr: &str,
    state: ApiState,
    shutdown: watch::Receiver<bool>,
) -> crate::Result<()> {
    serve_with(addr, state, shutdown, |routes| routes).await
}

/// [`serve`], with the embedding application's own pages next to the dashboard.
///
/// `extend` receives the token-gated router, so every route it adds answers
/// only with the dashboard token, like the dashboard does.
pub async fn serve_with<F>(
    addr: &str,
    state: ApiState,
    shutdown: watch::Receiver<bool>,
    extend: F,
) -> crate::Result<()>
where
    F: FnOnce(GatedRouter) -> GatedRouter,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|source| crate::Error::Bind {
            addr: addr.to_owned(),
            source: Box::new(source),
        })?;
    serve_on(listener, state, shutdown, extend).await
}

/// [`serve_with`] on a listener the caller already bound.
///
/// Port 0 with the address read back, or a socket handed over by the service
/// manager: binding first leaves no window in which another process can take
/// the port.
pub async fn serve_on<F>(
    listener: tokio::net::TcpListener,
    state: ApiState,
    mut shutdown: watch::Receiver<bool>,
    extend: F,
) -> crate::Result<()>
where
    F: FnOnce(GatedRouter) -> GatedRouter,
{
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
    #[cfg(feature = "game")]
    use rama_x_threejs::ThreeRouterExt as _;

    let app_router = {
        // Static assets are public; the pages that load them are not.
        let r = router(state.clone())
            // Datastar served from the binary, not a CDN.
            .with_get("/dashboard/datastar.js", dashboard::script)
            .with_get("/dashboard/datastar.js.map", dashboard::script_map);

        // three.js and Datastar, then the scene module and its models.
        #[cfg(feature = "game")]
        let r = r
            .with_three(game::THREE)
            .with_get("/game/game.js", game::scene)
            .with_get("/game/models/Textures/colormap.png", game::model_texture)
            .with_get(
                "/game/models/{name}",
                |Path(name): Path<String>| async move { game::model(&name) },
            );

        // Every route from here on shows job data, so answers only with the token.
        let r = r.with_endpoint_layer((
            DashboardToken::layer(&state),
            DefaultEndpointLayer::default(),
        ));

        #[cfg(feature = "game")]
        let r = r
            .with_get("/game", game::index)
            .with_get("/game/stream", game::stream);

        extend(
            r.with_get("/dashboard", dashboard::index)
                .with_get("/dashboard/stream", dashboard::stream),
        )
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

    let addr = listener
        .local_addr()
        .map_or_else(|_| "an unknown address".to_owned(), |a| a.to_string());
    let listener = TcpListener::from_tokio_tcp_listener(listener, exec);

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
