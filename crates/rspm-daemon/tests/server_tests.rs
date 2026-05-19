use rspm_core::api::RpcRequest;
use rspm_core::config::ProjectConfig;
use rspm_daemon::api::DaemonApi;
use rspm_daemon::runtime::TaskRuntime;
use rspm_daemon::server::handle_stream;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn tcp_json_line_transport_handles_task_list_request() {
    let temp = TempDir::new().expect("temp dir");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "server-test"

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
        handle_stream(stream, api).await.expect("handle stream");
    });

    let mut stream = TcpStream::connect(address).await.expect("connect");
    let request = RpcRequest::new(7, "task.list", serde_json::json!({}));
    let line = serde_json::to_string(&request).expect("request json");
    stream.write_all(line.as_bytes()).await.expect("write");
    stream.write_all(b"\n").await.expect("newline");

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .expect("read response");

    assert!(response.contains(r#""id":7"#));
    assert!(response.contains("master"));
}

#[cfg(unix)]
#[tokio::test]
async fn unix_socket_json_line_transport_handles_task_list_request() {
    use rspm_daemon::server::handle_unix_stream;
    use tokio::net::{UnixListener, UnixStream};

    let temp = TempDir::new().expect("temp dir");
    let socket_path = temp.path().join("rspmd.sock");
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "unix-server-test"

        [tasks.master]
        cmd = "true"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let listener = UnixListener::bind(&socket_path).expect("bind unix socket");

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_unix_stream(stream, api)
            .await
            .expect("handle stream");
    });

    let mut stream = UnixStream::connect(&socket_path).await.expect("connect");
    let request = RpcRequest::new(8, "task.list", serde_json::json!({}));
    let line = serde_json::to_string(&request).expect("request json");
    stream.write_all(line.as_bytes()).await.expect("write");
    stream.write_all(b"\n").await.expect("newline");

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .expect("read response");

    assert!(response.contains(r#""id":8"#));
    assert!(response.contains("master"));
}

#[cfg(windows)]
#[tokio::test]
async fn named_pipe_json_line_transport_handles_task_list_request() {
    use rspm_daemon::server::handle_named_pipe;
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    let temp = TempDir::new().expect("temp dir");
    let pipe_name = format!(r"\\.\pipe\rspm-server-test-{}", std::process::id());
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "named-pipe-server-test"

        [tasks.master]
        cmd = "noop"
        "#,
    )
    .expect("valid config");
    let runtime = TaskRuntime::new(config, temp.path()).expect("runtime");
    let api = DaemonApi::new(runtime);
    let pipe = ServerOptions::new()
        .first_pipe_instance(false)
        .create(&pipe_name)
        .expect("create named pipe");

    tokio::spawn(async move {
        pipe.connect().await.expect("accept named pipe");
        handle_named_pipe(pipe, api)
            .await
            .expect("handle named pipe");
    });

    let mut stream = ClientOptions::new()
        .open(&pipe_name)
        .expect("connect named pipe");
    let request = RpcRequest::new(9, "task.list", serde_json::json!({}));
    let line = serde_json::to_string(&request).expect("request json");
    stream.write_all(line.as_bytes()).await.expect("write");
    stream.write_all(b"\n").await.expect("newline");

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .expect("read response");

    assert!(response.contains(r#""id":9"#));
    assert!(response.contains("master"));
}
