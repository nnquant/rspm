use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use rspm_core::config::ProjectConfig;
use rspm_daemon::api::DaemonApi;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::server::serve_tcp;
use tempfile::TempDir;

fn write_config() -> (TempDir, String) {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("rspm.toml");
    fs::write(
        &path,
        r#"
        [project]
        name = "cli-test"

        [tasks.master]
        cmd = "true"

        [tasks.worker]
        cmd = "true"
        depends_on = ["master"]
        "#,
    )
    .expect("config");

    (temp, path.display().to_string())
}

#[test]
fn validate_accepts_valid_config() {
    let (_temp, path) = write_config();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["validate", "-f", &path])
        .output()
        .expect("run cli");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("valid"));
}

#[test]
fn apply_validates_config_and_prints_planned_task_count() {
    let (_temp, path) = write_config();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["apply", "-f", &path, "--dry-run"])
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("apply dry-run"));
    assert!(stdout.contains("tasks=2"));
    assert!(stdout.contains("master"));
    assert!(stdout.contains("worker"));
}

#[tokio::test]
async fn cli_apply_sends_config_to_daemon_when_addr_is_set() {
    let temp = TempDir::new().expect("temp dir");
    let initial = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-apply-test"

        [tasks.old]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(initial, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);
    let config_path = temp.path().join("rspm.toml");
    fs::write(
        &config_path,
        r#"
        [project]
        name = "cli-apply-test"

        [tasks.new]
        cmd = "true"
        "#,
    )
    .expect("write config");

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });
    wait_for_daemon(address).await;

    let apply = run_cli(&[
        "--addr",
        &address.to_string(),
        "apply",
        "-f",
        &config_path.display().to_string(),
    ])
    .await;
    let stdout = String::from_utf8_lossy(&apply.stdout);

    assert!(apply.status.success());
    assert!(stdout.contains("applied"));
    assert!(stdout.contains("new"));

    server.abort();
}

#[test]
fn graph_prints_dependency_edges() {
    let (_temp, path) = write_config();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["graph", "-f", &path])
        .output()
        .expect("run cli");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("master -> worker"));
}

#[test]
fn status_prints_docker_style_table_columns() {
    let (_temp, path) = write_config();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["--no-auto-daemon", "status", "-f", &path])
        .output()
        .expect("run cli");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("TASK_ID"));
    assert!(stdout.contains("NAME"));
    assert!(stdout.contains("MODE"));
    assert!(stdout.contains("PID"));
    assert!(stdout.contains("STATUS"));
    assert!(stdout.contains("START_TIME"));
    assert!(stdout.contains("STOP_TIME"));
    assert!(!stdout.contains("STARTED_UTC"));
    assert!(!stdout.contains("STOPPED_UTC"));
    assert!(stdout.contains("master"));
    assert!(stdout.contains("worker"));
    assert!(stdout.contains("oneshot"));
}

#[test]
fn ls_prints_docker_style_table_columns() {
    let (_temp, path) = write_config();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["--no-auto-daemon", "ls", "-f", &path])
        .output()
        .expect("run cli");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("TASK_ID"));
    assert!(stdout.contains("NAME"));
    assert!(stdout.contains("PID"));
    assert!(stdout.contains("STATUS"));
    assert!(stdout.contains("master"));
    assert!(stdout.contains("worker"));
}

#[tokio::test]
async fn cli_status_starts_daemon_when_it_is_not_running() {
    let (temp, path) = write_config();
    let address = free_tcp_addr();
    let log_dir = temp.path().join("logs");
    let state_dir = temp.path().join("state");
    let socket_path = temp.path().join("run").join("rspmd.sock");
    let address_text = address.to_string();
    let log_dir_text = log_dir.display().to_string();
    let state_dir_text = state_dir.display().to_string();
    let socket_path_text = socket_path.display().to_string();

    let output = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "status",
        "-f",
        &path,
    ])
    .await;
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("master"));
    assert!(stdout.contains("worker"));

    kill_daemon_from_state(&state_dir);
}

#[tokio::test]
async fn cli_status_requires_a_real_daemon_rpc_response_before_reusing_port() {
    let (temp, path) = write_config();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake listener");
    let address = listener.local_addr().expect("fake addr");
    let fake_server = tokio::spawn(async move {
        let _ = listener.accept().await;
    });
    let log_dir = temp.path().join("logs");
    let state_dir = temp.path().join("state");
    let socket_path = temp.path().join("run").join("rspmd.sock");
    let address_text = address.to_string();
    let log_dir_text = log_dir.display().to_string();
    let state_dir_text = state_dir.display().to_string();
    let socket_path_text = socket_path.display().to_string();

    let output = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "status",
        "-f",
        &path,
    ])
    .await;
    let stdout = String::from_utf8_lossy(&output.stdout);

    let _ = fake_server.await;
    assert!(output.status.success());
    assert!(stdout.contains("master"));
    assert!(stdout.contains("worker"));

    kill_daemon_from_state(&state_dir);
}

#[test]
fn service_install_dry_run_prints_platform_service_template() {
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args([
            "service",
            "install",
            "--dry-run",
            "--config",
            "examples/rspm.toml",
        ])
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("rspm daemon"));
    assert!(stdout.contains("examples/rspm.toml"));
    assert!(stdout.contains("127.0.0.1:27691"));
}

#[test]
fn service_install_writes_template_to_explicit_output_path() {
    let temp = TempDir::new().expect("temp dir");
    let output_path = temp.path().join("rspmd.service");
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args([
            "service",
            "install",
            "--config",
            "examples/rspm.toml",
            "--output",
            &output_path.display().to_string(),
        ])
        .output()
        .expect("run cli");
    let service = fs::read_to_string(&output_path).expect("service file");

    assert!(output.status.success());
    assert!(service.contains("rspm daemon"));
    assert!(service.contains("examples/rspm.toml"));
    assert!(service.contains("127.0.0.1:27691"));
}

#[test]
fn service_install_activate_dry_run_prints_platform_activation_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args([
            "service",
            "install",
            "--dry-run",
            "--activate",
            "--config",
            "examples/rspm.toml",
        ])
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("activation command"));
    if cfg!(target_os = "linux") {
        assert!(stdout.contains("systemctl --user"));
    }
}

#[tokio::test]
async fn cli_start_status_and_stop_operate_through_daemon_tcp_transport() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-daemon-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });

    wait_for_daemon(address).await;

    let start = run_cli(&["--addr", &address.to_string(), "start", "sleeper"]).await;
    let start_stdout = String::from_utf8_lossy(&start.stdout);
    assert!(start.status.success());
    assert!(start_stdout.contains("sleeper"));
    assert!(start_stdout.contains("TASK_ID"));

    let status = run_cli(&["--addr", &address.to_string(), "status"]).await;
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(status.status.success());
    assert!(stdout.contains("sleeper"));
    assert!(stdout.contains("online"));

    let stop = run_cli(&["--addr", &address.to_string(), "stop", "sleeper"]).await;
    let stop_stdout = String::from_utf8_lossy(&stop.stdout);
    assert!(stop.status.success());
    assert!(stop_stdout.contains("stopped"));
    assert!(stop_stdout.contains("TASK_ID"));

    server.abort();
}

#[tokio::test]
async fn cli_start_accepts_multiple_task_ids() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-task-id-test"

        [tasks.master]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.worker]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });
    wait_for_daemon(address).await;

    let start = run_cli(&["--addr", &address.to_string(), "start", "1", "2"]).await;
    let stdout = String::from_utf8_lossy(&start.stdout);

    assert!(start.status.success());
    assert!(stdout.contains("master online"));
    assert!(stdout.contains("worker online"));

    let stop = run_cli(&["--addr", &address.to_string(), "stop", "1", "2"]).await;
    assert!(stop.status.success());

    server.abort();
}

#[tokio::test]
async fn cli_reload_reports_not_configured_without_faking_zero_downtime() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-reload-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });
    wait_for_daemon(address).await;

    let output = run_cli(&["--addr", &address.to_string(), "reload", "master"]).await;
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("reload is not configured"));

    server.abort();
}

#[tokio::test]
async fn cli_start_all_and_stop_all_use_dag_order() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-all-test"

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
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });
    wait_for_daemon(address).await;

    let start = run_cli(&["--addr", &address.to_string(), "start", "all"]).await;
    let start_stdout = String::from_utf8_lossy(&start.stdout);
    assert!(start.status.success());
    assert!(start_stdout.contains("master online"));
    assert!(start_stdout.contains("worker online"));

    let stop = run_cli(&["--addr", &address.to_string(), "stop", "all"]).await;
    let stop_stdout = String::from_utf8_lossy(&stop.stdout);
    assert!(stop.status.success());
    assert!(stop_stdout.contains("worker stopped"));
    assert!(stop_stdout.contains("master stopped"));

    server.abort();
}

#[tokio::test]
async fn cli_logs_reads_daemon_managed_task_log() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-log-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf '\\033[32mINFO\\033[0m cli-log-line\\n'"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });

    wait_for_daemon(address).await;

    let start = run_cli(&["--addr", &address.to_string(), "start", "all"]).await;
    assert!(start.status.success());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let logs = run_cli(&["--addr", &address.to_string(), "logs", "echo"]).await;
    let stdout = String::from_utf8_lossy(&logs.stdout);

    assert!(logs.status.success());
    assert!(stdout.contains("echo | "));
    assert!(stdout.contains("cli-log-line"));
    assert!(stdout.contains("\x1b[32mINFO\x1b[0m"));

    let logs_by_id = run_cli(&["--addr", &address.to_string(), "logs", "1"]).await;
    let stdout_by_id = String::from_utf8_lossy(&logs_by_id.stdout);

    assert!(logs_by_id.status.success());
    assert!(stdout_by_id.contains("echo | "));
    assert!(stdout_by_id.contains("cli-log-line"));
    assert!(stdout_by_id.contains("\x1b[32mINFO\x1b[0m"));

    let log_by_id = run_cli(&["--addr", &address.to_string(), "log", "1", "--no-follow"]).await;
    let log_stdout_by_id = String::from_utf8_lossy(&log_by_id.stdout);

    assert!(log_by_id.status.success());
    assert!(log_stdout_by_id.contains("echo | "));
    assert!(log_stdout_by_id.contains("cli-log-line"));
    assert!(log_stdout_by_id.contains("\x1b[32mINFO\x1b[0m"));

    server.abort();
}

#[tokio::test]
async fn cli_status_colors_status_and_health_columns() {
    let temp = TempDir::new().expect("temp dir");
    let ready_path = temp.path().join("ready");
    fs::write(&ready_path, "ok").expect("ready file");
    let config = ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "cli-color-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "sleep 30"]

        [tasks.echo.health]
        type = "file"
        path = "{}"
        "#,
        ready_path.display()
    ))
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });
    wait_for_daemon(address).await;

    let start = run_cli(&["--addr", &address.to_string(), "start", "all"]).await;
    assert!(start.status.success());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let status = run_cli(&["--addr", &address.to_string(), "ls"]).await;
    let stdout = String::from_utf8_lossy(&status.stdout);

    assert!(status.status.success());
    assert!(stdout.contains("\x1b["));
    assert!(stdout.contains("healthy"));
    assert!(stdout.contains("ok"));

    let _ = run_cli(&["--addr", &address.to_string(), "stop", "echo"]).await;
    server.abort();
}

#[tokio::test]
async fn cli_events_prints_daemon_lifecycle_events() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-event-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf event"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });

    wait_for_daemon(address).await;

    let start = run_cli(&["--addr", &address.to_string(), "start", "echo"]).await;
    assert!(start.status.success());
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = run_cli(&["--addr", &address.to_string(), "events"]).await;
    let stdout = String::from_utf8_lossy(&events.stdout);

    assert!(events.status.success());
    assert!(stdout.contains("task_started"));
    assert!(stdout.contains("echo"));

    server.abort();
}

#[tokio::test]
async fn cli_doctor_reports_reachable_daemon() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-doctor-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address: SocketAddr = listener.local_addr().expect("addr");
    drop(listener);

    let server = tokio::spawn(async move {
        serve_tcp(&address.to_string(), api)
            .await
            .expect("serve tcp");
    });

    wait_for_daemon(address).await;

    let output = run_cli(&["--addr", &address.to_string(), "doctor"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("daemon: ok"));
    assert!(stdout.contains("tasks: 1"));

    server.abort();
}

async fn run_cli(args: &[&str]) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_rspm"));
    command.args(args);
    tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .expect("cli command timed out")
        .expect("run cli")
}

async fn wait_for_daemon(address: SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("daemon did not start at [{address}]");
}

fn free_tcp_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr")
}

fn kill_daemon_from_state(state_dir: &Path) {
    let pid_path = state_dir.join("rspmd.pid");
    let Ok(pid) = fs::read_to_string(pid_path) else {
        return;
    };
    if cfg!(target_os = "windows") {
        let _ = Command::new("taskkill")
            .args(["/PID", pid.trim(), "/F"])
            .status();
    } else {
        let _ = Command::new("kill").arg(pid.trim()).status();
    }
}
