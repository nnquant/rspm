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
fn cli_prints_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .arg("--version")
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("rspm"));
    assert!(stdout.contains(env!("CARGO_PKG_VERSION")));
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

#[test]
fn add_shell_command_writes_task_to_config() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("rspm.toml");
    let path_text = path.display().to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args([
            "--no-daemon",
            "add",
            "-f",
            &path_text,
            "--name",
            "alpha",
            "uv run a.py",
        ])
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let config_text = fs::read_to_string(&path).expect("config");

    assert!(output.status.success());
    assert!(stdout.contains("added [alpha] cmd=[uv] args=[run a.py]"));
    assert!(config_text.contains("[tasks.alpha]"));
    let config = ProjectConfig::from_toml_str(&config_text).expect("generated config");
    let task = config.task("alpha").expect("alpha task");
    assert_eq!(task.cmd, "uv");
    assert_eq!(task.args, vec!["run", "a.py"]);
}

#[test]
fn add_accepts_repeated_env_values_including_hyphen_prefixed_names() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("rspm.toml");
    let path_text = path.display().to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .env("ENV1", "one")
        .env("ENV2", "two")
        .args([
            "--no-daemon",
            "add",
            "-f",
            &path_text,
            "--name",
            "alpha",
            "--env",
            "ENV1",
            "--env",
            "--ENV2",
            "--env",
            "INLINE=three",
            "uv run a.py",
        ])
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "stdout=[{}] stderr=[{}]",
        stdout,
        stderr
    );
    let config_text = fs::read_to_string(&path).expect("config");
    let config = ProjectConfig::from_toml_str(&config_text).expect("generated config");
    let task = config.task("alpha").expect("alpha task");
    assert_eq!(task.env.get("ENV1").map(String::as_str), Some("one"));
    assert_eq!(task.env.get("ENV2").map(String::as_str), Some("two"));
    assert_eq!(task.env.get("INLINE").map(String::as_str), Some("three"));
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
        .args(["--no-daemon", "status", "-f", &path])
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
    assert!(stdout.contains("Timezone: local"));
    assert!(stdout.contains("master"));
    assert!(stdout.contains("worker"));
    assert!(stdout.contains("oneshot"));
}

#[test]
fn ls_prints_docker_style_table_columns() {
    let (_temp, path) = write_config();
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["--no-daemon", "ls", "-f", &path])
        .output()
        .expect("run cli");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("TASK_ID"));
    assert!(stdout.contains("NAME"));
    assert!(stdout.contains("PID"));
    assert!(stdout.contains("STATUS"));
    assert!(stdout.contains("Timezone: local"));
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
async fn cli_reports_missing_config_before_spawning_daemon() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("missing.toml");
    let log_dir = temp.path().join("logs");
    let state_dir = temp.path().join("state");
    let socket_path = temp.path().join("run").join("rspmd.sock");
    let address = free_tcp_addr();

    let output = run_cli(&[
        "--addr",
        &address.to_string(),
        "--log-dir",
        &log_dir.display().to_string(),
        "--state-dir",
        &state_dir.display().to_string(),
        "--socket-path",
        &socket_path.display().to_string(),
        "ls",
        "-f",
        &config_path.display().to_string(),
    ])
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("missing config"));
    assert!(stderr.contains("apply -f"));
    assert!(!state_dir.join("rspmd.pid").exists());
}

#[tokio::test]
async fn cli_monit_once_prints_monitor_snapshot() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("rspm.toml");
    let log_dir = temp.path().join("logs");
    let state_dir = temp.path().join("state");
    let socket_path = temp.path().join("run").join("rspmd.sock");
    fs::write(
        &config_path,
        r#"
        [project]
        name = "cli-monit-once-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("write config");

    let output = run_cli(&[
        "--addr",
        &free_tcp_addr().to_string(),
        "--log-dir",
        &log_dir.display().to_string(),
        "--state-dir",
        &state_dir.display().to_string(),
        "--socket-path",
        &socket_path.display().to_string(),
        "monit",
        "--once",
        "-f",
        &config_path.display().to_string(),
    ])
    .await;
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("MONIT"));
    assert!(stdout.contains("sleeper"));

    kill_daemon_from_state(&state_dir);
}

#[tokio::test]
async fn cli_daemon_start_stop_and_restart_manage_daemon_lifecycle() {
    let (temp, path) = write_config();
    let address = free_tcp_addr();
    let log_dir = temp.path().join("logs");
    let state_dir = temp.path().join("state");
    let socket_path = temp.path().join("run").join("rspmd.sock");
    let address_text = address.to_string();
    let log_dir_text = log_dir.display().to_string();
    let state_dir_text = state_dir.display().to_string();
    let socket_path_text = socket_path.display().to_string();

    let start = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "start",
        "-f",
        &path,
    ])
    .await;
    let start_stdout = String::from_utf8_lossy(&start.stdout);

    assert!(start.status.success());
    assert!(start_stdout.contains("daemon started"));
    assert!(tokio::net::TcpStream::connect(address).await.is_ok());

    let status = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "status",
    ])
    .await;
    let status_stdout = String::from_utf8_lossy(&status.stdout);

    assert!(status.status.success());
    assert!(status_stdout.contains("daemon: running"));
    assert!(status_stdout.contains(&address_text));

    let restart = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "restart",
        "-f",
        &path,
    ])
    .await;
    let restart_stdout = String::from_utf8_lossy(&restart.stdout);

    assert!(restart.status.success());
    assert!(restart_stdout.contains("daemon stopped"));
    assert!(restart_stdout.contains("daemon started"));
    assert!(tokio::net::TcpStream::connect(address).await.is_ok());

    let stop = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "stop",
    ])
    .await;
    let stop_stdout = String::from_utf8_lossy(&stop.stdout);

    assert!(stop.status.success());
    assert!(stop_stdout.contains("daemon stopped"));
    assert!(tokio::net::TcpStream::connect(address).await.is_err());

    let stopped_status = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "status",
    ])
    .await;
    let stopped_status_stdout = String::from_utf8_lossy(&stopped_status.stdout);

    assert!(stopped_status.status.success());
    assert!(stopped_status_stdout.contains("daemon: stopped"));
}

#[tokio::test]
async fn cli_daemon_restart_restores_running_tasks() {
    let temp = TempDir::new().expect("temp dir");
    let config_path = temp.path().join("rspm.toml");
    fs::write(
        &config_path,
        r#"
        [project]
        name = "daemon-restart-restore-test"

        [tasks.sleeper]
        cmd = "sh"
        args = ["-c", "sleep 30"]
        "#,
    )
    .expect("write config");
    let address = free_tcp_addr();
    let log_dir = temp.path().join("logs");
    let state_dir = temp.path().join("state");
    let socket_path = temp.path().join("run").join("rspmd.sock");
    let address_text = address.to_string();
    let log_dir_text = log_dir.display().to_string();
    let state_dir_text = state_dir.display().to_string();
    let socket_path_text = socket_path.display().to_string();
    let config_text = config_path.display().to_string();

    let start_daemon = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "start",
        "-f",
        &config_text,
    ])
    .await;
    assert!(start_daemon.status.success());

    let start_task = run_cli(&["--addr", &address_text, "start", "sleeper"]).await;
    assert!(start_task.status.success());

    let restart_daemon = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "restart",
        "-f",
        &config_text,
    ])
    .await;
    let restart_stdout = String::from_utf8_lossy(&restart_daemon.stdout);

    assert!(restart_daemon.status.success());
    assert!(restart_stdout.contains("task_id=1 sleeper online"));

    let status = run_cli(&["--addr", &address_text, "ls"]).await;
    let stdout = String::from_utf8_lossy(&status.stdout);

    assert!(status.status.success());
    assert!(stdout.contains("sleeper"));
    assert!(stdout.contains("online"));

    let _ = run_cli(&[
        "--addr",
        &address_text,
        "--log-dir",
        &log_dir_text,
        "--state-dir",
        &state_dir_text,
        "--socket-path",
        &socket_path_text,
        "daemon",
        "stop",
    ])
    .await;
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

#[test]
fn service_status_dry_run_prints_platform_status_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["service", "status", "--dry-run"])
        .output()
        .expect("run cli");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("status command"));
    if cfg!(target_os = "linux") {
        assert!(stdout.contains("systemctl --user status rspmd.service"));
    }
}

#[test]
fn service_start_stop_and_restart_dry_run_print_platform_commands() {
    let start = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["service", "start", "--dry-run"])
        .output()
        .expect("run cli");
    let stop = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["service", "stop", "--dry-run"])
        .output()
        .expect("run cli");
    let restart = Command::new(env!("CARGO_BIN_EXE_rspm"))
        .args(["service", "restart", "--dry-run"])
        .output()
        .expect("run cli");

    let start_stdout = String::from_utf8_lossy(&start.stdout);
    let stop_stdout = String::from_utf8_lossy(&stop.stdout);
    let restart_stdout = String::from_utf8_lossy(&restart.stdout);

    assert!(start.status.success());
    assert!(stop.status.success());
    assert!(restart.status.success());
    assert!(start_stdout.contains("start command"));
    assert!(stop_stdout.contains("stop command"));
    assert!(restart_stdout.contains("restart command"));
    if cfg!(target_os = "linux") {
        assert!(start_stdout.contains("systemctl --user start rspmd.service"));
        assert!(stop_stdout.contains("systemctl --user stop rspmd.service"));
        assert!(restart_stdout.contains("systemctl --user restart rspmd.service"));
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
    assert!(stop_stdout.contains("stopping"));
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
    assert!(stop_stdout.contains("worker stopping"));
    assert!(stop_stdout.contains("master stopping"));

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
        args = ["-c", "printf '\\033[32mINFO\\033[0m cli-log-line\\nDEBUG cli-log-tail\\n'"]

        [tasks.second]
        cmd = "sh"
        args = ["-c", "printf second-log-line\\n"]
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

    let aggregate_logs = run_cli(&["--addr", &address.to_string(), "logs"]).await;
    let aggregate_stdout = String::from_utf8_lossy(&aggregate_logs.stdout);

    assert!(aggregate_logs.status.success());
    assert!(aggregate_stdout.contains("echo | "));
    assert!(aggregate_stdout.contains("second | "));
    assert!(aggregate_stdout.contains("cli-log-line"));
    assert!(aggregate_stdout.contains("second-log-line"));

    let aggregate_log = run_cli(&["--addr", &address.to_string(), "log", "--no-follow"]).await;
    let aggregate_log_stdout = String::from_utf8_lossy(&aggregate_log.stdout);

    assert!(aggregate_log.status.success());
    assert!(aggregate_log_stdout.contains("echo | "));
    assert!(aggregate_log_stdout.contains("second | "));

    let tailed_logs = run_cli(&[
        "--addr",
        &address.to_string(),
        "logs",
        "echo",
        "--lines",
        "1",
    ])
    .await;
    let tailed_stdout = String::from_utf8_lossy(&tailed_logs.stdout);

    assert!(tailed_logs.status.success());
    assert!(!tailed_stdout.contains("cli-log-line"));
    assert!(tailed_stdout.contains("cli-log-tail"));

    let grepped_logs = run_cli(&[
        "--addr",
        &address.to_string(),
        "logs",
        "--grep",
        "second-log",
    ])
    .await;
    let grepped_stdout = String::from_utf8_lossy(&grepped_logs.stdout);

    assert!(grepped_logs.status.success());
    assert!(!grepped_stdout.contains("cli-log-line"));
    assert!(grepped_stdout.contains("second-log-line"));

    server.abort();
}

#[tokio::test]
async fn cli_logs_merge_orders_aggregate_logs_by_timestamp() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-log-merge-test"

        [tasks.alpha]
        cmd = "sh"
        args = ["-c", "printf '2026-05-19T00:00:02Z alpha-two\\n2026-05-19T00:00:04Z alpha-four\\n'"]

        [tasks.beta]
        cmd = "sh"
        args = ["-c", "printf '2026-05-19T00:00:01Z beta-one\\n2026-05-19T00:00:03Z beta-three\\n'"]
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

    let logs = run_cli(&["--addr", &address.to_string(), "logs", "--merge"]).await;
    let stdout = String::from_utf8_lossy(&logs.stdout);
    let beta_one = stdout.find("beta-one").expect("beta-one");
    let alpha_two = stdout.find("alpha-two").expect("alpha-two");
    let beta_three = stdout.find("beta-three").expect("beta-three");
    let alpha_four = stdout.find("alpha-four").expect("alpha-four");

    assert!(logs.status.success());
    assert!(beta_one < alpha_two);
    assert!(alpha_two < beta_three);
    assert!(beta_three < alpha_four);

    server.abort();
}

#[tokio::test]
async fn cli_logs_since_filters_timestamped_lines() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-log-since-test"

        [tasks.alpha]
        cmd = "sh"
        args = ["-c", "printf '2026-05-19T00:00:01Z old-line\\n2026-05-19T00:00:03Z new-line\\nwithout-time\\n'"]
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

    let logs = run_cli(&[
        "--addr",
        &address.to_string(),
        "logs",
        "alpha",
        "--since",
        "2026-05-19T00:00:02Z",
    ])
    .await;
    let stdout = String::from_utf8_lossy(&logs.stdout);

    assert!(logs.status.success());
    assert!(!stdout.contains("old-line"));
    assert!(stdout.contains("new-line"));
    assert!(!stdout.contains("without-time"));

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
    assert!(stdout.contains("platform:"));
    assert!(stdout.contains("default_addr:"));
    assert!(stdout.contains("auth_token:"));
    assert!(stdout.contains("service_status_command:"));
    assert!(stdout.contains("state_dir:"));
    assert!(stdout.contains("pid_file:"));
    assert!(stdout.contains("applied_config:"));
    assert!(stdout.contains("event_log:"));
    assert!(stdout.contains("socket_path:"));
    assert!(stdout.contains("tasks: 1"));

    server.abort();
}

#[tokio::test]
async fn cli_sends_configured_auth_token_to_daemon() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cli-auth-test"

        [tasks.echo]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime).with_auth_token("secret-token");
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

    let missing = run_cli(&["--addr", &address.to_string(), "--no-daemon", "ls"]).await;
    let accepted = run_cli(&[
        "--addr",
        &address.to_string(),
        "--token",
        "secret-token",
        "--no-daemon",
        "ls",
    ])
    .await;
    let accepted_stdout = String::from_utf8_lossy(&accepted.stdout);

    assert!(!missing.status.success());
    assert!(accepted.status.success());
    assert!(accepted_stdout.contains("echo"));

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
