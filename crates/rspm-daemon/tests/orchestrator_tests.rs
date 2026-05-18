use std::fs;

use rspm_core::config::{HealthCheck, HealthCheckKind, ProjectConfig};
use rspm_daemon::health::check_health;
use rspm_daemon::orchestrator::start_all;
use rspm_daemon::runtime::TaskRuntime;
use tempfile::TempDir;

#[tokio::test]
async fn command_and_file_health_checks_report_real_status() {
    let temp = TempDir::new().expect("temp dir");
    let ready_path = temp.path().join("ready");
    fs::write(&ready_path, "ok").expect("ready file");

    let command_health = HealthCheck {
        kind: HealthCheckKind::Command,
        address: None,
        url: None,
        command: Some("true".to_string()),
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
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "orchestrator-test"

        [tasks.master]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.master.health]
        type = "command"
        command = "false"

        [tasks.worker]
        cmd = "sh"
        args = ["-c", "printf worker"]
        depends_on = ["master"]
        start_when = "dependencies_healthy"
        "#,
    )
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
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.master.health]
        type = "file"
        path = "{}"
        "#,
        ready_path.display()
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let started = start_all(&mut runtime).await.expect("start all");

    assert_eq!(started[0].status, rspm_core::state::TaskStatus::Healthy);
    assert_eq!(started[0].health.as_deref(), Some("ok"));

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
        cmd = "sh"
        args = ["-c", "sleep 0.2; touch '{}'; sleep 30"]

        [tasks.master.health]
        type = "file"
        path = "{}"
        interval = "50ms"
        success_after = 1
        failure_after = 10
        "#,
        ready_path.display(),
        ready_path.display(),
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    let started = start_all(&mut runtime).await.expect("start all");

    assert_eq!(started[0].status, rspm_core::state::TaskStatus::Healthy);
    assert_eq!(started[0].health.as_deref(), Some("ok"));

    runtime.stop_task("master").await.expect("cleanup master");
}
