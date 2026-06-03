use std::fs;

use chrono::{TimeZone, Utc};
use rspm_core::config::{HealthCheck, HealthCheckKind, ProjectConfig};
use rspm_daemon::health::check_health;
use rspm_daemon::orchestrator::{start_all, start_scheduled_active};
use rspm_daemon::runtime::TaskRuntime;
use tempfile::TempDir;

mod common;

#[tokio::test]
async fn command_and_file_health_checks_report_real_status() {
    let temp = TempDir::new().expect("temp dir");
    let ready_path = temp.path().join("ready");
    fs::write(&ready_path, "ok").expect("ready file");

    let command_health = HealthCheck {
        kind: HealthCheckKind::Command,
        address: None,
        url: None,
        command: Some(common::health_success_command().to_string()),
        path: None,
        interval: None,
        timeout: None,
        success_after: None,
        failure_after: None,
    };
    let file_health = HealthCheck {
        kind: HealthCheckKind::File,
        address: None,
        url: None,
        command: None,
        path: Some(ready_path.display().to_string()),
        interval: None,
        timeout: None,
        success_after: None,
        failure_after: None,
    };

    assert!(check_health(&command_health).await.expect("command health"));
    assert!(check_health(&file_health).await.expect("file health"));
}

#[tokio::test]
async fn dependency_task_does_not_start_when_upstream_health_fails() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "orchestrator-test"

        [tasks.master]
        {}

        [tasks.master.health]
        type = "command"
        command = "{}"

        [tasks.worker]
        {}
        depends_on = ["master"]
        start_when = "dependencies_healthy"
        "#,
        common::sleep_task_command(),
        common::toml_string(common::health_failure_command()),
        common::print_task_command("worker")
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let error = start_all(&mut runtime)
        .await
        .expect_err("health failure blocks worker");

    assert!(error.to_string().contains("master"));
    assert!(runtime
        .describe_task("master")
        .expect("master")
        .pid
        .is_some());
    assert!(runtime
        .describe_task("worker")
        .expect("worker")
        .pid
        .is_none());

    runtime.stop_task("master").await.expect("cleanup master");
}

#[tokio::test]
async fn successful_health_probe_marks_task_healthy() {
    let temp = TempDir::new().expect("temp dir");
    let ready_path = temp.path().join("ready");
    fs::write(&ready_path, "ok").expect("ready file");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "healthy-status-test"

        [tasks.master]
        {}

        [tasks.master.health]
        type = "file"
        path = "{}"
        "#,
        common::sleep_task_command(),
        common::toml_path(&ready_path)
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let started = start_all(&mut runtime).await.expect("start all");

    assert_eq!(started[0].status, rspm_core::state::TaskStatus::Healthy);
    assert_eq!(started[0].health.as_deref(), Some("ok"));

    runtime.stop_task("master").await.expect("cleanup master");
}

#[tokio::test]
async fn scheduled_active_startup_starts_task_and_dependencies() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "scheduled-active-startup"
        timezone = "Asia/Shanghai"

        [tasks.master]
        {}

        [tasks.market]
        {}
        depends_on = ["master"]
        start_when = "dependencies_started"

        [tasks.market.schedule]
        start = "30 8 * * *"
        stop = "00 15 * * *"
        "#,
        common::sleep_task_command(),
        common::sleep_task_command()
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let started = start_scheduled_active(
        &mut runtime,
        Utc.with_ymd_and_hms(2026, 5, 18, 1, 30, 0).unwrap(),
    )
    .await
    .expect("scheduled active startup");

    assert_eq!(started.len(), 2);
    assert!(runtime
        .describe_task("master")
        .expect("master")
        .pid
        .is_some());
    assert!(runtime
        .describe_task("market")
        .expect("market")
        .pid
        .is_some());

    runtime.stop_task("market").await.expect("cleanup market");
    runtime.stop_task("master").await.expect("cleanup master");
}

#[tokio::test]
async fn startup_health_probe_waits_for_delayed_ready_file() {
    let temp = TempDir::new().expect("temp dir");
    let ready_path = temp.path().join("delayed-ready");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "delayed-health-test"

        [tasks.master]
        {}

        [tasks.master.health]
        type = "file"
        path = "{}"
        interval = "50ms"
        success_after = 1
        failure_after = 10
        "#,
        common::sleep_then_touch_task_command(&ready_path),
        common::toml_path(&ready_path),
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let started = start_all(&mut runtime).await.expect("start all");

    assert_eq!(started[0].status, rspm_core::state::TaskStatus::Healthy);
    assert_eq!(started[0].health.as_deref(), Some("ok"));

    runtime.stop_task("master").await.expect("cleanup master");
}
