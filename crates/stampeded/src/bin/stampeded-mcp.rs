//! Stampeded MCP server over stdio.
//!
//!   STAMPEDED_DATABASE_URL=postgres://... stampeded-mcp
//!   STAMPEDED_MCP_ALLOW_WRITES=1 stampeded-mcp   # mount job_retry / job_cancel
//!
//! Point an MCP client at this command. Read tools work for any session; the
//! write tools additionally require the session to carry an `operator` role.

use mcp_host::SESSION_ROLES_KEY;
use mcp_host::transport::StdioTransport;
use stampeded::Queue;
use stampeded::mcp::{StampededServer, router, server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // stderr, never stdout: stdout IS the MCP transport, and a stray log line
    // there corrupts the JSON-RPC stream.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stampeded_mcp=info".into()),
        )
        .init();

    let url =
        std::env::var("STAMPEDED_DATABASE_URL").map_err(|_| "STAMPEDED_DATABASE_URL is required")?;
    let allow_writes = std::env::var("STAMPEDED_MCP_ALLOW_WRITES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let pool = sqlx::PgPool::connect(&url).await?;
    let state = StampededServer::new(Queue::new(pool));

    // Build first, then mount: mcp-host attaches a router to an already-built
    // server rather than taking state through the builder.
    let mut mcp = server("stampeded", env!("CARGO_PKG_VERSION")).build();

    // The write tools are gated on the session carrying an `operator` role,
    // and over stdio nothing else can grant one -- there is no auth layer to
    // do it, so without this they would be mounted and permanently invisible.
    //
    // On stdio the operator IS whoever launched the process: it is a local,
    // single-client pipe, and STAMPEDED_MCP_ALLOW_WRITES is their decision. On
    // the HTTP transport the role comes from the authenticating proxy instead,
    // which is why this lives in the binary rather than in `router()`.
    if allow_writes {
        mcp.set_session_initializer(|session| {
            session.set_state(SESSION_ROLES_KEY, serde_json::json!(["operator"]));
        });
    }
    mcp.register_router(router(allow_writes), std::sync::Arc::new(state));

    tracing::info!(allow_writes, "stampeded mcp server ready");
    mcp.run(StdioTransport::new()).await?;
    Ok(())
}
