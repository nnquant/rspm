use rspm_core::api::RpcRequest;
use rspm_core::config::ProjectConfig;
use rspm_core::event::{EventType, TaskEvent};
use rspm_core::state::TaskStatus;
use rspm_daemon::api::DaemonApi;
use rspm_daemon::runtime::TaskRuntime;
use tempfile::TempDir;

#[tokio::test]
async fn daemon_api_handles_start_describe_list_and_stop() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    let start = api
        .handle(RpcRequest::start("sleeper"))
        .await
        .expect("start response");
    assert!(start.error.is_none());

    let describe = api
        .handle(RpcRequest::new(
            2,
            "task.describe",
            serde_json::json!({ "task": "sleeper" }),
        ))
        .await
        .expect("describe response");
    let info: rspm_core::state::TaskInfo =
        serde_json::from_value(describe.result.expect("result")).expect("task info");
    assert_eq!(info.status, TaskStatus::Online);
    assert!(info.pid.is_some());

    let list = api
        .handle(RpcRequest::new(3, "task.list", serde_json::json!({})))
        .await
        .expect("list response");
    assert!(list.result.expect("result").is_array());

    let stop = api
        .handle(RpcRequest::new(
            4,
            "task.stop",
            serde_json::json!({ "task": "sleeper" }),
        ))
        .await
        .expect("stop response");
    assert!(stop.error.is_none());
}

#[tokio::test]
async fn daemon_api_rejects_requests_with_missing_or_wrong_token() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-auth-test"

        [tasks.echo]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime).with_auth_token("secret-token");

    let missing = api
        .handle(RpcRequest::new(1, "task.list", serde_json::json!({})))
        .await
        .expect("missing token response");
    let wrong = api
        .handle(RpcRequest::new(
            2,
            "task.list",
            serde_json::json!({ "token": "wrong-token" }),
        ))
        .await
        .expect("wrong token response");
    let accepted = api
        .handle(RpcRequest::new(
            3,
            "task.list",
            serde_json::json!({ "token": "secret-token" }),
        ))
        .await
        .expect("accepted response");

    assert_eq!(missing.error.expect("error").code, -32001);
    assert_eq!(wrong.error.expect("error").code, -32001);
    assert!(accepted.error.is_none());
}

#[tokio::test]
async fn daemon_api_starts_and_stops_all_tasks_in_dag_order() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-all-test"

        [tasks.master]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.worker]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        depends_on = ["master"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    let start = api
        .handle(RpcRequest::new(1, "task.start_all", serde_json::json!({})))
        .await
        .expect("start all");
    let started: Vec<rspm_core::state::TaskInfo> =
        serde_json::from_value(start.result.expect("result")).expect("started tasks");

    assert_eq!(started[0].name, "master");
    assert_eq!(started[1].name, "worker");

    let stop = api
        .handle(RpcRequest::new(2, "task.stop_all", serde_json::json!({})))
        .await
        .expect("stop all");
    let stopped: Vec<rspm_core::state::TaskInfo> =
        serde_json::from_value(stop.result.expect("result")).expect("stopped tasks");

    assert_eq!(stopped[0].name, "worker");
    assert_eq!(stopped[1].name, "master");
}

#[tokio::test]
async fn daemon_api_returns_error_response_for_unknown_task() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-error-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    let response = api
        .handle(RpcRequest::new(
            9,
            "task.start",
            serde_json::json!({ "task": "missing" }),
        ))
        .await
        .expect("rpc response");

    assert!(response.result.is_none());
    assert!(response.error.expect("error").message.contains("missing"));
}

#[tokio::test]
async fn daemon_api_reload_returns_explicit_not_configured_error() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-reload-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    let response = api
        .handle(RpcRequest::new(
            10,
            "task.reload",
            serde_json::json!({ "task": "master" }),
        ))
        .await
        .expect("reload response");

    assert!(response.result.is_none());
    assert!(response
        .error
        .expect("error")
        .message
        .contains("reload is not configured"));
}

#[tokio::test]
async fn daemon_api_reload_runs_configured_command() {
    let temp = TempDir::new().expect("temp dir");
    let marker = temp.path().join("reloaded");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "api-reload-command-test"

        [tasks.master]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.master.reload]
        mode = "command"
        command = "touch {}"
        "#,
        marker.display()
    ))
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    api.handle(RpcRequest::start("master"))
        .await
        .expect("start");
    let response = api
        .handle(RpcRequest::new(
            11,
            "task.reload",
            serde_json::json!({ "task": "master" }),
        ))
        .await
        .expect("reload response");

    assert!(response.error.is_none());
    assert!(marker.exists());

    api.handle(RpcRequest::new(
        12,
        "task.stop",
        serde_json::json!({ "task": "master" }),
    ))
    .await
    .expect("cleanup");
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_api_reload_sends_configured_signal() {
    let temp = TempDir::new().expect("temp dir");
    let marker = temp.path().join("signal-reloaded");
    let script = format!(
        "trap 'touch {}' USR1; while true; do sleep 0.05; done",
        marker.display()
    );
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "api-reload-signal-test"

        [tasks.master]
        cmd = "sh"
        args = ["-c", "{}"]

        [tasks.master.reload]
        mode = "signal"
        signal = "USR1"
        "#,
        script.replace('"', "\\\"")
    ))
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    api.handle(RpcRequest::start("master"))
        .await
        .expect("start");
    let response = api
        .handle(RpcRequest::new(
            11,
            "task.reload",
            serde_json::json!({ "task": "master" }),
        ))
        .await
        .expect("reload response");

    assert!(response.error.is_none());
    for _ in 0..20 {
        if marker.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(marker.exists());

    api.handle(RpcRequest::new(
        12,
        "task.stop",
        serde_json::json!({ "task": "master" }),
    ))
    .await
    .expect("cleanup");
}

#[tokio::test]
async fn daemon_api_applies_new_config_and_stops_removed_tasks() {
    let temp = TempDir::new().expect("temp dir");
    let initial = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "apply-test"

        [tasks.old]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(initial, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    api.handle(RpcRequest::start("old"))
        .await
        .expect("start old");
    let response = api
        .handle(RpcRequest::new(
            2,
            "config.apply",
            serde_json::json!({
                "toml": r#"
                    [project]
                    name = "apply-test"

                    [tasks.new]
                    cmd = "true"
                "#
            }),
        ))
        .await
        .expect("apply response");
    let tasks: Vec<rspm_core::state::TaskInfo> =
        serde_json::from_value(response.result.expect("result")).expect("task list");

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].name, "new");
    assert_eq!(tasks[0].status, TaskStatus::Stopped);

    let events = api
        .handle(RpcRequest::new(3, "event.list", serde_json::json!({})))
        .await
        .expect("events");
    let events: Vec<TaskEvent> =
        serde_json::from_value(events.result.expect("result")).expect("events json");
    assert!(events
        .iter()
        .any(|event| event.event_type == EventType::ConfigApplied));
}

#[tokio::test]
async fn daemon_api_persists_applied_config_when_store_is_configured() {
    let temp = TempDir::new().expect("temp dir");
    let initial = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "persist-apply-test"

        [tasks.old]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let applied_path = temp.path().join("state").join("applied.toml");
    let runtime = TaskRuntime::new(initial, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime).with_applied_config_path(&applied_path);
    let toml = r#"
        [project]
        name = "persist-apply-test"

        [tasks.new]
        cmd = "true"
    "#;

    let response = api
        .handle(RpcRequest::new(
            2,
            "config.apply",
            serde_json::json!({ "toml": toml }),
        ))
        .await
        .expect("apply response");

    assert!(response.error.is_none());
    assert!(std::fs::read_to_string(applied_path)
        .expect("applied config")
        .contains("[tasks.new]"));
}

#[tokio::test]
async fn daemon_api_writes_lifecycle_events_to_jsonl_log() {
    let temp = TempDir::new().expect("temp dir");
    let event_path = temp.path().join("events").join("project.jsonl");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "event-jsonl-test"

        [tasks.echo]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path())
        .expect("runtime")
        .with_event_log_path(&event_path);
    let mut api = DaemonApi::new(runtime);

    api.handle(RpcRequest::start("echo")).await.expect("start");
    api.handle(RpcRequest::new(
        2,
        "task.wait",
        serde_json::json!({ "task": "echo" }),
    ))
    .await
    .expect("wait");

    let events = std::fs::read_to_string(event_path).expect("event log");
    assert!(events.contains("task_started"));
    assert!(events.contains("task_stopped"));
}

#[tokio::test]
async fn runtime_restores_lifecycle_state_from_event_log() {
    let temp = TempDir::new().expect("temp dir");
    let event_path = temp.path().join("events").join("project.jsonl");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "event-restore-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf restore"]
        "#,
    )
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config.clone(), temp.path())
        .expect("runtime")
        .with_event_log_path(&event_path);

    runtime.start_task("echo").await.expect("start");
    let stopped = runtime.wait_task_exit("echo").await.expect("wait");
    assert!(stopped.started_at.is_some());
    assert!(stopped.stopped_at.is_some());

    let restored = TaskRuntime::new(config, temp.path())
        .expect("runtime")
        .with_event_log_path(&event_path);
    let info = restored.describe_task("echo").expect("describe");

    assert!(info.started_at.is_some());
    assert!(info.stopped_at.is_some());
    assert!(info.uptime_ms.is_some());
    assert!(!restored.list_events().is_empty());
}

#[tokio::test]
async fn runtime_reattaches_running_task_pid_from_event_log() {
    let temp = TempDir::new().expect("temp dir");
    let event_path = temp.path().join("events").join("project.jsonl");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "reattach-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config.clone(), temp.path())
        .expect("runtime")
        .with_event_log_path(&event_path);
    let started = runtime.start_task("sleeper").await.expect("start");
    let started_pid = started.pid.expect("pid");
    drop(runtime);

    let mut restored = TaskRuntime::new(config, temp.path())
        .expect("runtime")
        .with_event_log_path(&event_path);
    let info = restored.describe_task("sleeper").expect("describe");

    assert_eq!(info.pid, Some(started_pid));
    assert_eq!(info.status, rspm_core::state::TaskStatus::Online);

    restored.stop_task("sleeper").await.expect("cleanup");
}

#[tokio::test]
async fn daemon_api_lists_task_lifecycle_events() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-event-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf event"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    api.handle(RpcRequest::start("echo")).await.expect("start");
    api.handle(RpcRequest::new(
        2,
        "task.wait",
        serde_json::json!({ "task": "echo" }),
    ))
    .await
    .expect("wait");
    let response = api
        .handle(RpcRequest::new(3, "event.list", serde_json::json!({})))
        .await
        .expect("events");
    let events: Vec<rspm_core::event::TaskEvent> =
        serde_json::from_value(response.result.expect("result")).expect("events json");

    assert!(events
        .iter()
        .any(|event| event.event_type == EventType::TaskStarted));
    assert!(events
        .iter()
        .any(|event| event.event_type == EventType::TaskStopped));
}

#[tokio::test]
async fn daemon_api_returns_task_logs() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "api-log-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf hello-from-log"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);

    api.handle(RpcRequest::start("echo")).await.expect("start");
    api.handle(RpcRequest::new(
        2,
        "task.wait",
        serde_json::json!({ "task": "echo" }),
    ))
    .await
    .expect("wait");
    let logs = api
        .handle(RpcRequest::new(
            3,
            "task.logs",
            serde_json::json!({ "task": "echo" }),
        ))
        .await
        .expect("logs");

    assert_eq!(
        logs.result.expect("result").as_str(),
        Some("hello-from-log")
    );
}
