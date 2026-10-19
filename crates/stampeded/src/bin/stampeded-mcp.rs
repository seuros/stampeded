//! Stampeded MCP server over stdio.
//!
//!   STAMPEDED_DATABASE_URL=postgres://... stampeded-mcp
//!   STAMPEDED_MCP_ALLOW_WRITES=1 stampeded-mcp   # mount job_retry / job_cancel
//!
//! Write tools also require the session to carry an `operator` role.

use mcp_host::SESSION_ROLES_KEY;
use mcp_host::transport::StdioTransport;
use stampeded::Queue;
use stampeded::mcp::{StampededServer, router, server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // stderr, never stdout: stdout is the MCP transport.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stampeded_mcp=info".into()),
        )
        .init();

    let url = std::env::var("STAMPEDED_DATABASE_URL")
        .map_err(|_| "STAMPEDED_DATABASE_URL is required")?;
    let allow_writes = std::env::var("STAMPEDED_MCP_ALLOW_WRITES")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));

    let pool = sqlx::PgPool::connect(&url).await?;
    let state = StampededServer::new(Queue::new(pool));

    let mut mcp = server("stampeded", env!("CARGO_PKG_VERSION"), allow_writes).build();

    // Over stdio nothing else grants `operator`: whoever launched the process is
    // the operator. On HTTP the authenticating proxy sets the role instead.
    if allow_writes {
        mcp.set_session_initializer(|_, session| {
            session.set_state(SESSION_ROLES_KEY, serde_json::json!(["operator"]));
        });
    }
    mcp.register_router(router(), std::sync::Arc::new(state))?;

    tracing::info!(allow_writes, "stampeded mcp server ready");
    mcp.run(StdioTransport::new()).await?;
    Ok(())
}
