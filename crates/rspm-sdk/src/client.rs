use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rspm_core::config::ProjectConfig;
use rspm_core::dag::TaskGraph;
use rspm_core::event::TaskEvent;
use rspm_core::state::TaskInfo;
use rspm_daemon::runtime::TaskRuntime;

use crate::transport::TcpRspmClient;

pub struct RspmClient {
    runtime: TaskRuntime,
}

impl RspmClient {
    pub async fn connect_default() -> Result<TcpRspmClient> {
        TcpRspmClient::connect(std::net::SocketAddr::from_str("127.0.0.1:27691")?).await
    }

    pub fn from_runtime(runtime: TaskRuntime) -> Self {
        Self { runtime }
    }

    pub async fn start(&mut self, task: &str) -> Result<TaskInfo> {
        self.runtime.start_task(task).await
    }

    pub async fn stop(&mut self, task: &str) -> Result<TaskInfo> {
        self.runtime.stop_task(task).await
    }

    pub async fn restart(&mut self, task: &str) -> Result<TaskInfo> {
        self.runtime.restart_task(task).await
    }

    pub async fn reload(&mut self, task: &str) -> Result<TaskInfo> {
        self.runtime.reload_task(task).await
    }

    pub async fn apply_file(&mut self, path: impl AsRef<Path>) -> Result<Vec<TaskInfo>> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config [{}]", path.display()))?;
        let config = ProjectConfig::from_toml_str(&text)?;
        let _ = TaskGraph::from_config(&config)?.plan_all()?;
        self.runtime.apply_config(config).await
    }

    pub async fn wait_task_exit(&mut self, task: &str) -> Result<TaskInfo> {
        self.runtime.wait_task_exit(task).await
    }

    pub async fn list_tasks(&self) -> Result<Vec<TaskInfo>> {
        self.runtime.list_tasks()
    }

    pub async fn describe_task(&self, task: &str) -> Result<TaskInfo> {
        self.runtime.describe_task(task)
    }

    pub async fn logs(&self, task: &str) -> Result<String> {
        self.runtime.read_task_log(task)
    }

    pub async fn events(&self) -> Result<Vec<TaskEvent>> {
        Ok(self.runtime.list_events())
    }

    pub async fn wait_healthy(&mut self, task: &str, timeout: Duration) -> Result<TaskInfo> {
        let deadline = Instant::now() + timeout;
        loop {
            let _ = self.runtime.reconcile_health_checks().await?;
            let info = self.runtime.describe_task(task)?;
            if info.status == rspm_core::state::TaskStatus::Healthy
                || (info.pid.is_some() && info.health.is_none())
            {
                return Ok(info);
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for task [{task}] to become healthy");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
