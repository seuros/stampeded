//! Stampede MCP server over stdio.
//!
//!   STAMPEDE_DATABASE_URL=postgres://... stampede-mcp
//!   STAMPEDE_MCP_ALLOW_WRITES=1 stampede-mcp   # mount job_retry / job_cancel
//!
//! Point an MCP client at this command. Read tools work for any session; the
//! write tools additionally require the session to carry an `operator` role.

use mcp_host::transport::StdioTransport;
use stampede_core::Queue;
use stampede_mcp::{StampedeServer, router, server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // stderr, never stdout: stdout IS the MCP transport, and a stray log line
    // there corrupts the JSON-RPC stream.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stampede_mcp=info".into()),
        )
        .init();

    let url = std::env::var("STAMPEDE_DATABASE_URL")
        .map_err(|_| "STAMPEDE_DATABASE_URL is required")?;
    let allow_writes = std::env::var("STAMPEDE_MCP_ALLOW_WRITES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let pool = sqlx::PgPool::connect(&url).await?;
    let state = StampedeServer::new(Queue::new(pool));

    // Build first, then mount: mcp-host attaches a router to an already-built
    // server rather than taking state through the builder.
    let mcp = server("stampede", env!("CARGO_PKG_VERSION")).build();
    mcp.register_router(router(allow_writes), std::sync::Arc::new(state));

    tracing::info!(allow_writes, "stampede mcp server ready");
    mcp.run(StdioTransport::new()).await?;
    Ok(())
}
