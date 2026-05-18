use std::sync::Arc;

use anyhow::{Context, Result};
use rspm_core::api::{RpcRequest, RpcResponse};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::api::DaemonApi;

pub async fn serve_tcp(address: &str, mut api: DaemonApi) -> Result<()> {
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind daemon address [{address}]"))?;

    loop {
        let (stream, _) = listener.accept().await?;
        api = handle_stream(stream, api).await?;
    }
}

pub async fn serve_tcp_shared(address: &str, api: Arc<Mutex<DaemonApi>>) -> Result<()> {
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind daemon address [{address}]"))?;

    loop {
        let (stream, _) = listener.accept().await?;
        let api = api.clone();
        tokio::spawn(async move {
            let _ = handle_shared_stream(stream, api).await;
        });
    }
}

pub async fn handle_stream(stream: TcpStream, api: DaemonApi) -> Result<DaemonApi> {
    let (reader, writer) = stream.into_split();
    handle_io(reader, writer, api).await
}

pub async fn handle_shared_stream(stream: TcpStream, api: Arc<Mutex<DaemonApi>>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await? != 0 {
        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => api.lock().await.handle(request).await?,
            Err(error) => RpcResponse::error(0, -32700, format!("parse error: {error}")),
        };
        let payload = serde_json::to_string(&response)?;
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        line.clear();
    }

    Ok(())
}

#[cfg(unix)]
pub async fn serve_unix(path: &std::path::Path, mut api: DaemonApi) -> Result<()> {
    use tokio::net::UnixListener;

    if path.exists() {
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("failed to remove stale unix socket [{}]", path.display()))?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("failed to bind unix socket [{}]", path.display()))?;

    loop {
        let (stream, _) = listener.accept().await?;
        api = handle_unix_stream(stream, api).await?;
    }
}

#[cfg(unix)]
pub async fn serve_unix_shared(path: &std::path::Path, api: Arc<Mutex<DaemonApi>>) -> Result<()> {
    use tokio::net::UnixListener;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create unix socket directory [{}]",
                parent.display()
            )
        })?;
    }
    if path.exists() {
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("failed to remove stale unix socket [{}]", path.display()))?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("failed to bind unix socket [{}]", path.display()))?;

    loop {
        let (stream, _) = listener.accept().await?;
        let api = api.clone();
        tokio::spawn(async move {
            let _ = handle_shared_unix_stream(stream, api).await;
        });
    }
}

#[cfg(unix)]
pub async fn handle_unix_stream(
    stream: tokio::net::UnixStream,
    api: DaemonApi,
) -> Result<DaemonApi> {
    let (reader, writer) = stream.into_split();
    handle_io(reader, writer, api).await
}

#[cfg(unix)]
pub async fn handle_shared_unix_stream(
    stream: tokio::net::UnixStream,
    api: Arc<Mutex<DaemonApi>>,
) -> Result<()> {
    handle_shared_io(stream, api).await
}

async fn handle_shared_io<S>(stream: S, api: Arc<Mutex<DaemonApi>>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await? != 0 {
        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => api.lock().await.handle(request).await?,
            Err(error) => RpcResponse::error(0, -32700, format!("parse error: {error}")),
        };
        let payload = serde_json::to_string(&response)?;
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        line.clear();
    }

    Ok(())
}

#[cfg(windows)]
pub async fn serve_named_pipe(name: &str, mut api: DaemonApi) -> Result<()> {
    loop {
        let pipe = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(false)
            .create(name)
            .with_context(|| format!("failed to create named pipe [{name}]"))?;
        pipe.connect()
            .await
            .with_context(|| format!("failed to accept named pipe [{name}]"))?;
        api = handle_named_pipe(pipe, api).await?;
    }
}

#[cfg(windows)]
pub async fn serve_named_pipe_shared(name: &str, api: Arc<Mutex<DaemonApi>>) -> Result<()> {
    loop {
        let pipe = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(false)
            .create(name)
            .with_context(|| format!("failed to create named pipe [{name}]"))?;
        pipe.connect()
            .await
            .with_context(|| format!("failed to accept named pipe [{name}]"))?;
        let api = api.clone();
        tokio::spawn(async move {
            let _ = handle_shared_named_pipe(pipe, api).await;
        });
    }
}

#[cfg(windows)]
pub async fn handle_named_pipe(
    pipe: tokio::net::windows::named_pipe::NamedPipeServer,
    api: DaemonApi,
) -> Result<DaemonApi> {
    let (reader, writer) = tokio::io::split(pipe);
    handle_io(reader, writer, api).await
}

#[cfg(windows)]
pub async fn handle_shared_named_pipe(
    pipe: tokio::net::windows::named_pipe::NamedPipeServer,
    api: Arc<Mutex<DaemonApi>>,
) -> Result<()> {
    handle_shared_io(pipe, api).await
}

async fn handle_io<R, W>(reader: R, mut writer: W, mut api: DaemonApi) -> Result<DaemonApi>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await? != 0 {
        let response = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => api.handle(request).await?,
            Err(error) => RpcResponse::error(0, -32700, format!("parse error: {error}")),
        };
        let payload = serde_json::to_string(&response)?;
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        line.clear();
    }

    Ok(api)
}
