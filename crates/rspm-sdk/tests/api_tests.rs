use rspm_core::config::ProjectConfig;
use rspm_core::state::TaskStatus;
use rspm_daemon::runtime::TaskRuntime;
use rspm_sdk::api::{RpcRequest, RpcResponse};
use rspm_sdk::RspmClient;
use tempfile::TempDir;

#[path = "../../rspm-daemon/tests/common/mod.rs"]
mod common;

#[test]
fn serializes_json_rpc_style_start_request() {
    let request = RpcRequest::start("master");
    let json = serde_json::to_string(&request).expect("json");

    assert!(json.contains(r#""jsonrpc":"2.0""#));
    assert!(json.contains(r#""method":"task.start""#));
    assert!(json.contains(r#""task":"master""#));
}

#[test]
fn serializes_success_response_with_task_payload() {
    let response = RpcResponse::success(1, serde_json::json!({"ok": true}));
    let json = serde_json::to_string(&response).expect("json");

    assert!(json.contains(r#""jsonrpc":"2.0""#));
    assert!(json.contains(r#""result":{"ok":true}"#));
}

#[tokio::test]
async fn in_process_client_starts_and_lists_tasks() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "sdk-test"

        [tasks.echo]
        {}
        "#,
        common::print_task_command("sdk")
    ))
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut client = RspmClient::from_runtime(runtime);

    client.start("echo").await.expect("start");
    let stopped = client.wait_task_exit("echo").await.expect("exit");
    let tasks = client.list_tasks().await.expect("list");

    assert_eq!(stopped.status, TaskStatus::Stopped);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].name, "echo");
}

#[tokio::test]
async fn in_process_client_reads_aggregate_task_logs() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "sdk-aggregate-logs-test"

        [tasks.alpha]
        {}

        [tasks.beta]
        {}
        "#,
        common::print_task_command("alpha-log"),
        common::print_task_command("beta-log")
    ))
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut client = RspmClient::from_runtime(runtime);

    client.start("alpha").await.expect("start alpha");
    client.wait_task_exit("alpha").await.expect("wait alpha");
    client.start("beta").await.expect("start beta");
    client.wait_task_exit("beta").await.expect("wait beta");
    let logs = client.logs_all().await.expect("logs all");

    assert_eq!(logs.get("alpha").map(String::as_str), Some("alpha-log"));
    assert_eq!(logs.get("beta").map(String::as_str), Some("beta-log"));
}
