use std::net::SocketAddr;
use std::path::PathBuf;

use rspm_core::config::ProjectConfig;
use rspm_daemon::api::DaemonApi;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::server::handle_stream;
use rspm_sdk::{DaemonOwnership, RspmSupervisor};
use tempfile::TempDir;
use tokio::net::TcpListener;

#[test]
fn supervisor_builds_detached_daemon_command() {
    let address: SocketAddr = "127.0.0.1:39001".parse().expect("addr");
    let supervisor = RspmSupervisor::new()
        .addr(address)
        .binary_path("/opt/rspm/bin/rspm")
        .log_dir(".myapp/rspm/logs")
        .state_dir(".myapp/rspm/state")
        .socket_path(".myapp/rspm/run/rspmd.sock")
        .token("secret-token");

    let spec = supervisor.daemon_command_spec("tasks.rspm.toml");

    assert_eq!(supervisor.ownership(), DaemonOwnership::Detached);
    assert_eq!(spec.program, PathBuf::from("/opt/rspm/bin/rspm"));
    assert_eq!(
        spec.args,
        vec![
            "daemon",
            "run",
            "tasks.rspm.toml",
            "127.0.0.1:39001",
            ".myapp/rspm/logs",
            ".myapp/rspm/state",
            ".myapp/rspm/run/rspmd.sock",
            "--token",
            "secret-token",
        ]
    );
}

#[tokio::test]
async fn supervisor_reuses_running_daemon_without_spawning() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "supervisor-reuse-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_stream(stream, api).await.expect("stream");
    });

    let supervisor = RspmSupervisor::new()
        .addr(address)
        .binary_path("/path/that/must/not/be/spawned");
    let mut client = supervisor
        .ensure_daemon("missing-config.toml")
        .await
        .expect("connect existing daemon");
    let tasks = client.list_tasks().await.expect("list tasks");

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].name, "master");
}
