use std::fs;
use std::time::Duration;

use rspm_core::config::ProjectConfig;
use rspm_core::state::TaskStatus;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::runtime::{cpu_percent_from_proc_samples, ProcCpuSample};
use tempfile::TempDir;

fn config(input: &str) -> ProjectConfig {
    ProjectConfig::from_toml_str(input).expect("valid config")
}

#[test]
fn computes_process_cpu_percent_from_proc_samples() {
    let sample = ProcCpuSample {
        process_ticks: 150,
        process_start_ticks_since_boot: 1_000,
        uptime_ticks_since_boot: 2_000,
        clock_ticks_per_second: 100,
        cpu_count: 2,
    };

    let percent = cpu_percent_from_proc_samples(sample).expect("cpu percent");

    assert!((percent - 7.5).abs() < 0.001);
}

#[tokio::test]
async fn starts_task_and_captures_stdout_and_stderr_logs() {
    let temp = TempDir::new().expect("temp dir");
    let config = config(
        r#"
        [project]
        name = "supervisor-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf hello; printf err >&2"]
        "#,
    );
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("echo").await.expect("start task");
    let info = runtime.wait_task_exit("echo").await.expect("wait exit");

    assert_eq!(info.status, TaskStatus::Stopped);
    assert_eq!(info.last_exit_code, Some(0));
    assert!(info.started_at.is_some());
    assert!(info.stopped_at.is_some());
    assert!(info.uptime_ms.is_some());
    assert_eq!(info.run_mode, "oneshot");

    let log = fs::read_to_string(runtime.log_path("echo")).expect("task log");
    assert!(log.contains("hello"));
    assert!(log.contains("err"));
}

#[tokio::test]
async fn rotates_task_log_when_size_limit_is_reached() {
    let temp = TempDir::new().expect("temp dir");
    let config = config(
        r#"
        [project]
        name = "log-rotation-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf new-log"]

        [tasks.echo.logs]
        max_bytes = 3
        "#,
    );
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let log_path = runtime.log_path("echo");
    fs::write(&log_path, "old-log").expect("old log");

    runtime.start_task("echo").await.expect("start task");
    runtime.wait_task_exit("echo").await.expect("wait exit");

    assert_eq!(
        fs::read_to_string(log_path.with_extension("log.1")).expect("rotated log"),
        "old-log"
    );
    assert_eq!(fs::read_to_string(log_path).expect("new log"), "new-log");
}

#[tokio::test]
async fn stops_long_running_task_and_updates_task_info() {
    let temp = TempDir::new().expect("temp dir");
    let config = config(
        r#"
        [project]
        name = "supervisor-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    );
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("sleeper").await.expect("start task");
    let running = runtime.describe_task("sleeper").expect("running info");

    assert_eq!(running.status, TaskStatus::Online);
    assert!(running.pid.is_some());

    let stopped = runtime.stop_task("sleeper").await.expect("stop task");

    assert_eq!(stopped.status, TaskStatus::Stopped);
    assert!(stopped.pid.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn stop_task_sends_term_before_force_kill() {
    let temp = TempDir::new().expect("temp dir");
    let marker = temp.path().join("terminated");
    let config = config(&format!(
        r#"
        [project]
        name = "graceful-stop-test"

        [defaults]
        kill_timeout = "2s"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "trap 'printf term > {}; exit 0' TERM; while true; do sleep 1; done"]
        "#,
        marker.display()
    ));
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("sleeper").await.expect("start task");
    let stopped = runtime.stop_task("sleeper").await.expect("stop task");

    assert_eq!(stopped.status, TaskStatus::Stopped);
    assert_eq!(stopped.last_exit_code, Some(0));
    assert_eq!(fs::read_to_string(marker).expect("marker"), "term");
}

#[tokio::test]
async fn restarts_failed_task_according_to_restart_policy() {
    let temp = TempDir::new().expect("temp dir");
    let marker = temp.path().join("first-run");
    let script = format!(
        "if [ -f '{}' ]; then sleep 30; else touch '{}'; exit 1; fi",
        marker.display(),
        marker.display()
    );
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "restart-test"

        [defaults]
        restart = "on-failure"
        max_restarts = 2

        [tasks.flaky]
        cmd = "sh"
        args = ["-c", "{}"]
        "#,
        script.replace('"', "\\\"")
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("flaky").await.expect("start task");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let restarted = runtime
        .reconcile_exited_tasks()
        .await
        .expect("reconcile exits");
    let info = runtime.describe_task("flaky").expect("task info");

    assert_eq!(restarted.len(), 1);
    assert_eq!(info.status, TaskStatus::Online);
    assert_eq!(info.restart_count, 1);
    assert!(info.pid.is_some());

    runtime.stop_task("flaky").await.expect("cleanup");
}

#[tokio::test]
async fn automatic_restart_respects_configured_restart_delay() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "restart-delay-test"

        [defaults]
        restart = "always"
        restart_delay = "20ms"

        [tasks.flaky]
        cmd = "false"
        "#,
    )
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("flaky").await.expect("start task");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = std::time::Instant::now();
    let restarted = runtime
        .reconcile_exited_tasks()
        .await
        .expect("reconcile exits");

    assert_eq!(restarted.len(), 1);
    assert!(started.elapsed() >= Duration::from_millis(20));

    let _ = runtime.wait_task_exit("flaky").await;
}

#[tokio::test]
async fn restarts_task_when_watched_file_changes() {
    let temp = TempDir::new().expect("temp dir");
    let watched = temp.path().join("watched.txt");
    fs::write(&watched, "first").expect("watched file");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "watch-test"

        [tasks.worker]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.worker.watch]
        paths = ["{}"]
        "#,
        watched.display()
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("worker").await.expect("start worker");
    let before = runtime.describe_task("worker").expect("before");
    runtime.snapshot_watch_state().expect("snapshot");
    tokio::time::sleep(Duration::from_millis(5)).await;
    fs::write(&watched, "second").expect("modify watched file");
    let restarted = runtime
        .reconcile_watch_changes()
        .await
        .expect("watch restart");
    let after = runtime.describe_task("worker").expect("after");

    assert_eq!(restarted.len(), 1);
    assert_ne!(before.pid, after.pid);

    runtime.stop_task("worker").await.expect("cleanup");
}

#[tokio::test]
async fn restarts_task_when_memory_limit_is_exceeded() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "memory-test"

        [tasks.worker]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.worker.limits]
        max_memory_bytes = 10
        "#,
    )
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("worker").await.expect("start worker");
    let before = runtime.describe_task("worker").expect("before");
    let restarted = runtime
        .reconcile_memory_limits(|_| Some(20))
        .await
        .expect("memory restart");
    let after = runtime.describe_task("worker").expect("after");

    assert_eq!(restarted.len(), 1);
    assert_ne!(before.pid, after.pid);

    runtime.stop_task("worker").await.expect("cleanup");
}
