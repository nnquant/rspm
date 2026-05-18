use rspm_core::api::RpcRequest;
use rspm_core::config::ProjectConfig;
use rspm_core::state::TaskStatus;
use rspm_daemon::api::DaemonApi;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::server::handle_stream;
use rspm_sdk::TcpRspmClient;
use tempfile::TempDir;
use tokio::net::TcpListener;

#[tokio::test]
async fn tcp_sdk_client_sends_request_to_daemon_transport() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "transport-test"

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

    let mut client = TcpRspmClient::connect(address).await.expect("connect");
    let response = client
        .send(RpcRequest::new(11, "task.list", serde_json::json!({})))
        .await
        .expect("response");

    assert_eq!(response.id, 11);
    assert!(response.error.is_none());
    assert!(response.result.expect("result").is_array());
}

#[tokio::test]
async fn tcp_sdk_client_exposes_high_level_task_operations() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "transport-task-test"

        [tasks.echo]
        cmd = "sh"
        args = ["-c", "printf tcp-sdk"]
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let mut api = DaemonApi::new(runtime);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            api = handle_stream(stream, api).await.expect("stream");
        }
    });

    let mut client = TcpRspmClient::connect(address).await.expect("connect");
    let started = client.start("echo").await.expect("start");
    let stopped = client.wait("echo").await.expect("wait");
    let logs = client.logs("echo").await.expect("logs");
    let events = client.events().await.expect("events");

    assert_eq!(started.status, TaskStatus::Online);
    assert_eq!(stopped.status, TaskStatus::Stopped);
    assert!(logs.contains("tcp-sdk"));
    assert!(!events.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn unix_sdk_client_sends_request_to_daemon_transport() {
    use rspm_daemon::server::handle_unix_stream;
    use rspm_sdk::UnixRspmClient;
    use tokio::net::UnixListener;

    let temp = TempDir::new().expect("temp dir");
    let socket_path = temp.path().join("rspmd.sock");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "unix-transport-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = UnixListener::bind(&socket_path).expect("bind");

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_unix_stream(stream, api).await.expect("stream");
    });

    let mut client = UnixRspmClient::connect(&socket_path)
        .await
        .expect("connect");
    let tasks = client.list_tasks().await.expect("list tasks");

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].name, "master");
}
