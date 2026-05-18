use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rspm_core::api::{RpcRequest, RpcResponse};
use rspm_core::event::TaskEvent;
use rspm_core::state::{TaskInfo, TaskStatus};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct TcpRspmClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TcpRspmClient {
    pub async fn connect(address: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(address)
            .await
            .with_context(|| format!("failed to connect rspmd at [{address}]"))?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn send(&mut self, request: RpcRequest) -> Result<RpcResponse> {
        let payload = serde_json::to_string(&request)?;
        self.writer.write_all(payload.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;

        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        Ok(serde_json::from_str(&line)?)
    }

    pub async fn start(&mut self, task: &str) -> Result<TaskInfo> {
        self.task_action("task.start", task).await
    }

    pub async fn stop(&mut self, task: &str) -> Result<TaskInfo> {
        self.task_action("task.stop", task).await
    }

    pub async fn restart(&mut self, task: &str) -> Result<TaskInfo> {
        self.task_action("task.restart", task).await
    }

    pub async fn reload(&mut self, task: &str) -> Result<TaskInfo> {
        self.task_action("task.reload", task).await
    }

    pub async fn wait(&mut self, task: &str) -> Result<TaskInfo> {
        self.task_action("task.wait", task).await
    }

    pub async fn describe(&mut self, task: &str) -> Result<TaskInfo> {
        self.task_action("task.describe", task).await
    }

    pub async fn list_tasks(&mut self) -> Result<Vec<TaskInfo>> {
        let response = self
            .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
            .await?;
        decode_result(response)
    }

    pub async fn logs(&mut self, task: &str) -> Result<String> {
        let response = self
            .send(RpcRequest::new(
                1,
                "task.logs",
                serde_json::json!({ "task": task }),
            ))
            .await?;
        decode_result(response)
    }

    pub async fn tail_logs(&mut self, task: &str) -> Result<String> {
        self.logs(task).await
    }

    pub async fn events(&mut self) -> Result<Vec<TaskEvent>> {
        let response = self
            .send(RpcRequest::new(1, "event.list", serde_json::json!({})))
            .await?;
        decode_result(response)
    }

    pub async fn watch_events(&mut self) -> Result<Vec<TaskEvent>> {
        self.events().await
    }

    pub async fn apply_toml(&mut self, toml_text: &str) -> Result<Vec<TaskInfo>> {
        let response = self
            .send(RpcRequest::new(
                1,
                "config.apply",
                serde_json::json!({ "toml": toml_text }),
            ))
            .await?;
        decode_result(response)
    }

    pub async fn apply_file(&mut self, path: impl AsRef<std::path::Path>) -> Result<Vec<TaskInfo>> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config [{}]", path.display()))?;
        self.apply_toml(&text).await
    }

    pub async fn wait_status(
        &mut self,
        task: &str,
        status: TaskStatus,
        timeout: Duration,
    ) -> Result<TaskInfo> {
        let deadline = Instant::now() + timeout;
        loop {
            let info = self.describe(task).await?;
            if info.status == status {
                return Ok(info);
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for task [{task}] status [{status:?}]");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_healthy(&mut self, task: &str, timeout: Duration) -> Result<TaskInfo> {
        let deadline = Instant::now() + timeout;
        loop {
            let info = self.describe(task).await?;
            if info.status == TaskStatus::Healthy || (info.pid.is_some() && info.health.is_none()) {
                return Ok(info);
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for task [{task}] to become healthy");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn task_action(&mut self, method: &str, task: &str) -> Result<TaskInfo> {
        let response = self
            .send(RpcRequest::new(
                1,
                method,
                serde_json::json!({ "task": task }),
            ))
            .await?;
        decode_result(response)
    }
}

#[cfg(unix)]
pub struct UnixRspmClient {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

#[cfg(unix)]
impl UnixRspmClient {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let stream = tokio::net::UnixStream::connect(path)
            .await
            .with_context(|| format!("failed to connect rspmd socket [{}]", path.display()))?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn send(&mut self, request: RpcRequest) -> Result<RpcResponse> {
        send_json_line(&mut self.reader, &mut self.writer, request).await
    }

    pub async fn list_tasks(&mut self) -> Result<Vec<TaskInfo>> {
        let response = self
            .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
            .await?;
        decode_result(response)
    }
}

#[cfg(windows)]
pub struct NamedPipeRspmClient {
    reader: BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    writer: tokio::io::WriteHalf<tokio::net::windows::named_pipe::NamedPipeClient>,
}

#[cfg(windows)]
impl NamedPipeRspmClient {
    pub async fn connect(name: &str) -> Result<Self> {
        let pipe = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(name)
            .with_context(|| format!("failed to connect rspmd named pipe [{name}]"))?;
        let (reader, writer) = tokio::io::split(pipe);
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn send(&mut self, request: RpcRequest) -> Result<RpcResponse> {
        send_json_line(&mut self.reader, &mut self.writer, request).await
    }

    pub async fn list_tasks(&mut self) -> Result<Vec<TaskInfo>> {
        let response = self
            .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
            .await?;
        decode_result(response)
    }
}

async fn send_json_line<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    request: RpcRequest,
) -> Result<RpcResponse>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&request)?;
    writer.write_all(payload.as_bytes()).await?;
    writer.write_all(b"\n").await?;

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(&line)?)
}

fn decode_result<T>(response: RpcResponse) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}
