use super::*;
use mcp_host::SESSION_ROLES_KEY;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const READS: [&str; 7] = [
    "failure_summary",
    "job_inspect",
    "job_search",
    "queue_status",
    "schedule_list",
    "system_health",
    "worker_status",
];
const WRITES: [&str; 5] = [
    "job_cancel",
    "job_retry",
    "queue_pause",
    "schedule_pause",
    "schedule_upsert",
];

/// A server as `stampeded-mcp` builds it, one initialized session with `roles`.
async fn session(allow_writes: bool, roles: &[&str]) -> (mcp_host::server::Server, Value) {
    let srv = server("stampeded-test", "0", allow_writes).build();
    // MOCKED: a pool that never connects; listing and refused calls touch no database.
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/never").expect("lazy pool");
    srv.register_router(router(), Arc::new(StampededServer::new(Queue::new(pool))))
        .expect("register");

    let roles = json!(roles);
    let init = json!({
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": { "name": "test", "version": "0" }
    });
    call(&srv, &roles, "initialize", init).await;
    srv.handle_request_with_session_state(
        "s",
        JsonRpcRequest::notification("notifications/initialized", None),
        [(SESSION_ROLES_KEY, roles.clone())],
    )
    .await;
    (srv, roles)
}

async fn call(srv: &mcp_host::server::Server, roles: &Value, method: &str, params: Value) -> Value {
    let response = srv
        .handle_request_with_session_state(
            "s",
            JsonRpcRequest::new(json!(1), method, Some(params)),
            [(SESSION_ROLES_KEY, roles.clone())],
        )
        .await
        .expect("a response");
    serde_json::to_value(response).expect("serializable")
}

async fn listed(allow_writes: bool, roles: &[&str]) -> BTreeSet<String> {
    let (srv, roles) = session(allow_writes, roles).await;
    let response = call(&srv, &roles, "tools/list", json!({})).await;
    response["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools in {response}"))
        .iter()
        .map(|t| t["name"].as_str().expect("a name").to_string())
        .collect()
}

#[tokio::test]
async fn write_tools_need_a_writing_process_and_an_operator() {
    let reads: BTreeSet<String> = READS.iter().map(ToString::to_string).collect();
    let all: BTreeSet<String> = READS
        .iter()
        .chain(&WRITES)
        .map(ToString::to_string)
        .collect();

    for (allow_writes, roles, expected) in [
        (false, &[][..], &reads),
        (false, &["operator"][..], &reads),
        (true, &[][..], &reads),
        (true, &["viewer"][..], &reads),
        (true, &["operator"][..], &all),
    ] {
        assert_eq!(
            &listed(allow_writes, roles).await,
            expected,
            "allow_writes={allow_writes} roles={roles:?}"
        );
    }
}

#[tokio::test]
async fn a_hidden_write_tool_cannot_be_called_either() {
    let (srv, roles) = session(true, &["viewer"]).await;
    let args = json!({ "queue": "default", "id": "01a15345-1d8b-77e7-b7a7-d38da92fc9fe" });
    let response = call(
        &srv,
        &roles,
        "tools/call",
        json!({ "name": "job_cancel", "arguments": args }),
    )
    .await;

    assert_eq!(
        response["error"]["data"]["code"],
        json!("TOOL_NOT_FOUND"),
        "a viewer reached job_cancel: {response}"
    );
}

#[tokio::test]
async fn every_tool_takes_an_object_and_says_whether_it_writes() {
    let (srv, roles) = session(true, &["operator"]).await;
    let response = call(&srv, &roles, "tools/list", json!({})).await;
    for tool in response["result"]["tools"].as_array().expect("tools") {
        let name = tool["name"].as_str().expect("a name");
        assert_eq!(
            tool["inputSchema"]["type"],
            json!("object"),
            "{name}: {tool}"
        );
        let read_only = tool["annotations"]["readOnlyHint"] == json!(true);
        assert_eq!(read_only, READS.contains(&name), "{name}: {tool}");
    }
}
