use chrono::{TimeZone, Utc};
use rspm_core::config::ProjectConfig;
use rspm_core::state::TaskStatus;
use rspm_daemon::api::DaemonApi;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::scheduler::run_due_actions;
use tempfile::TempDir;

#[tokio::test]
async fn scheduler_tick_starts_due_task() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "scheduler-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.sleeper.schedule]
        start = "30 8 * * *"
        "#,
    )
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let last = Utc.with_ymd_and_hms(2026, 5, 18, 8, 29, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 5, 18, 8, 30, 0).unwrap();

    let actions = run_due_actions(&mut runtime, last, now)
        .await
        .expect("run due actions");
    let info = runtime.describe_task("sleeper").expect("task info");

    assert_eq!(actions.len(), 1);
    assert_eq!(info.status, TaskStatus::Online);
    assert!(info.pid.is_some());

    runtime.stop_task("sleeper").await.expect("cleanup");
}

#[tokio::test]
async fn task_info_reports_next_scheduled_action() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "next-info-test"
        timezone = "Asia/Shanghai"

        [tasks.market]
        cmd = "true"

        [tasks.market.schedule]
        start = "30 8 * * *"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let info = runtime
        .describe_task_at(
            "market",
            Utc.with_ymd_and_hms(2026, 5, 18, 0, 0, 0).unwrap(),
        )
        .expect("task info");

    assert_eq!(
        info.schedule_state.as_deref(),
        Some("start 05-18 00:30:00Z")
    );
}

#[tokio::test]
async fn daemon_maintenance_tick_runs_scheduler_actions() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "maintenance-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.sleeper.schedule]
        start = "30 8 * * *"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);
    let last = Utc.with_ymd_and_hms(2026, 5, 18, 8, 29, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 5, 18, 8, 30, 0).unwrap();

    api.maintenance_tick(last, now)
        .await
        .expect("maintenance tick");
    let response = api
        .handle(rspm_core::api::RpcRequest::new(
            1,
            "task.describe",
            serde_json::json!({ "task": "sleeper" }),
        ))
        .await
        .expect("describe");
    let info: rspm_core::state::TaskInfo =
        serde_json::from_value(response.result.expect("result")).expect("task info");

    assert_eq!(info.status, TaskStatus::Online);
    assert!(info.pid.is_some());

    api.handle(rspm_core::api::RpcRequest::new(
        2,
        "task.stop",
        serde_json::json!({ "task": "sleeper" }),
    ))
    .await
    .expect("cleanup");
}
