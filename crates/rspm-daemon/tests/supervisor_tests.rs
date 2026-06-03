use std::fs;
use std::time::{Duration, Instant};

use rspm_core::config::ProjectConfig;
use rspm_core::state::TaskStatus;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::runtime::{cpu_percent_from_proc_samples, ProcCpuSample};
use tempfile::TempDir;

mod common;

fn config(input: &str) -> ProjectConfig {
    ProjectConfig::from_toml_str(input).expect("valid config")
}

async fn reconcile_until_status(
    runtime: &mut TaskRuntime,
    task_name: &str,
    expected: TaskStatus,
) -> rspm_core::state::TaskInfo {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let _ = runtime
            .reconcile_exited_tasks()
            .await
            .expect("reconcile exits");
        let info = runtime.describe_task(task_name).expect("task info");
        if info.status == expected {
            return info;
        }
        assert!(
            Instant::now() < deadline,
            "task [{task_name}] did not reach status [{expected:?}], last status [{:?}]",
            info.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn reconcile_until_restart(
    runtime: &mut TaskRuntime,
    task_name: &str,
) -> Vec<rspm_core::state::TaskInfo> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let restarted = runtime
            .reconcile_exited_tasks()
            .await
            .expect("reconcile exits");
        if !restarted.is_empty() {
            return restarted;
        }
        assert!(
            Instant::now() < deadline,
            "task [{task_name}] did not restart before deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
    let config = config(&format!(
        r#"
        [project]
        name = "supervisor-test"

        [tasks.echo]
        {}
        "#,
        common::print_stdout_stderr_task_command("hello", "err")
    ));
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
    let config = config(&format!(
        r#"
        [project]
        name = "log-rotation-test"

        [tasks.echo]
        {}

        [tasks.echo.logs]
        max_bytes = 3
        "#,
        common::print_task_command("new-log")
    ));
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
    let config = config(&format!(
        r#"
        [project]
        name = "supervisor-test"

        [tasks.sleeper]
        {}
        "#,
        common::sleep_task_command()
    ));
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("sleeper").await.expect("start task");
    let running = runtime.describe_task("sleeper").expect("running info");

    assert_eq!(running.status, TaskStatus::Online);
    assert!(running.pid.is_some());

    let stopping = runtime.stop_task("sleeper").await.expect("stop task");

    assert_eq!(stopping.status, TaskStatus::Stopping);
    assert!(stopping.pid.is_some());

    let stopped = runtime.wait_task_exit("sleeper").await.expect("wait stop");
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
    let stopping = runtime.stop_task("sleeper").await.expect("stop task");
    let stopped = runtime.wait_task_exit("sleeper").await.expect("wait stop");

    assert_eq!(stopping.status, TaskStatus::Stopping);
    assert_eq!(stopped.status, TaskStatus::Stopped);
    assert_eq!(stopped.last_exit_code, Some(0));
    assert_eq!(fs::read_to_string(marker).expect("marker"), "term");
}

#[cfg(unix)]
#[tokio::test]
async fn stop_task_returns_stopping_without_waiting_for_kill_timeout() {
    let temp = TempDir::new().expect("temp dir");
    let config = config(
        r#"
        [project]
        name = "async-stop-test"

        [defaults]
        kill_timeout = "500ms"

        [tasks.sleeper]
        cmd = "python3"
        args = ["-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"]
        "#,
    );
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("sleeper").await.expect("start task");
    let started = std::time::Instant::now();
    let stopping = runtime.stop_task("sleeper").await.expect("stop task");

    assert!(
        started.elapsed() < Duration::from_millis(200),
        "stop_task should return before kill_timeout elapses"
    );
    assert_eq!(stopping.status, TaskStatus::Stopping);
    assert!(stopping.pid.is_some());

    tokio::time::sleep(Duration::from_millis(550)).await;
    let info = reconcile_until_status(&mut runtime, "sleeper", TaskStatus::Stopped).await;
    assert_eq!(info.status, TaskStatus::Stopped);
}

#[cfg(unix)]
#[tokio::test]
async fn restart_task_returns_restarting_without_waiting_for_kill_timeout() {
    let temp = TempDir::new().expect("temp dir");
    let config = config(
        r#"
        [project]
        name = "async-restart-test"

        [defaults]
        kill_timeout = "500ms"

        [tasks.sleeper]
        cmd = "python3"
        args = ["-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"]
        "#,
    );
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("sleeper").await.expect("start task");
    let started = std::time::Instant::now();
    let restarting = runtime.restart_task("sleeper").await.expect("restart task");

    assert!(
        started.elapsed() < Duration::from_millis(200),
        "restart_task should return before kill_timeout elapses"
    );
    assert_eq!(restarting.status, TaskStatus::Restarting);
    assert!(restarting.pid.is_some());

    let old_pid = restarting.pid;
    tokio::time::sleep(Duration::from_millis(550)).await;
    let restarted = reconcile_until_status(&mut runtime, "sleeper", TaskStatus::Online).await;
    assert_ne!(restarted.pid, old_pid);

    runtime.stop_task("sleeper").await.expect("cleanup");
    tokio::time::sleep(Duration::from_millis(550)).await;
    let _ = reconcile_until_status(&mut runtime, "sleeper", TaskStatus::Stopped).await;
}

#[tokio::test]
async fn restarts_failed_task_according_to_restart_policy() {
    let temp = TempDir::new().expect("temp dir");
    let marker = temp.path().join("first-run");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "restart-test"

        [defaults]
        restart = "on-failure"
        max_restarts = 2

        [tasks.flaky]
        {}
        "#,
        common::flaky_once_task_command(&marker)
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("flaky").await.expect("start task");
    let restarted = reconcile_until_restart(&mut runtime, "flaky").await;
    let info = runtime.describe_task("flaky").expect("task info");

    assert_eq!(restarted.len(), 1);
    assert_eq!(info.status, TaskStatus::Online);
    assert_eq!(info.restart_count, 1);
    assert!(info.pid.is_some());

    runtime.stop_task("flaky").await.expect("cleanup");
    runtime.wait_task_exit("flaky").await.expect("cleanup exit");
}

#[tokio::test]
async fn automatic_restart_respects_configured_restart_delay() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "restart-delay-test"

        [defaults]
        restart = "always"
        restart_delay = "20ms"

        [tasks.flaky]
        {}
        "#,
        common::failure_task_command()
    ))
    .expect("valid config");
    let mut runtime = TaskRuntime::new(config, temp.path()).expect("runtime");

    runtime.start_task("flaky").await.expect("start task");
    let started = std::time::Instant::now();
    let restarted = reconcile_until_restart(&mut runtime, "flaky").await;

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

        [defaults]
        kill_timeout = "100ms"

        [tasks.worker]
        {}

        [tasks.worker.watch]
        paths = ["{}"]
        "#,
        common::sleep_task_command(),
        common::toml_path(&watched)
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
    assert_eq!(after.status, TaskStatus::Restarting);
    assert_eq!(before.pid, after.pid);

    tokio::time::sleep(Duration::from_millis(150)).await;
    let after_restart = reconcile_until_status(&mut runtime, "worker", TaskStatus::Online).await;
    assert_ne!(before.pid, after_restart.pid);

    runtime.stop_task("worker").await.expect("cleanup");
    runtime
        .wait_task_exit("worker")
        .await
        .expect("cleanup exit");
}

#[tokio::test]
async fn restarts_task_when_memory_limit_is_exceeded() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "memory-test"

        [defaults]
        kill_timeout = "100ms"

        [tasks.worker]
        {}

        [tasks.worker.limits]
        max_memory_bytes = 10
        "#,
        common::sleep_task_command()
    ))
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
    assert_eq!(after.status, TaskStatus::Restarting);
    assert_eq!(before.pid, after.pid);

    tokio::time::sleep(Duration::from_millis(150)).await;
    let after_restart = reconcile_until_status(&mut runtime, "worker", TaskStatus::Online).await;
    assert_ne!(before.pid, after_restart.pid);

    runtime.stop_task("worker").await.expect("cleanup");
    runtime
        .wait_task_exit("worker")
        .await
        .expect("cleanup exit");
}
