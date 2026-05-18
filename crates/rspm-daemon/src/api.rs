use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rspm_core::api::{RpcRequest, RpcResponse};
use rspm_core::dag::TaskGraph;
use serde::Deserialize;

use rspm_core::config::ProjectConfig;

use crate::orchestrator::{start_all, stop_all};
use crate::runtime::TaskRuntime;
use crate::scheduler::run_due_actions;

pub struct DaemonApi {
    runtime: TaskRuntime,
    applied_config_path: Option<PathBuf>,
}

impl DaemonApi {
    pub fn new(runtime: TaskRuntime) -> Self {
        Self {
            runtime,
            applied_config_path: None,
        }
    }

    pub fn with_applied_config_path(mut self, path: impl AsRef<Path>) -> Self {
        self.applied_config_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub async fn handle(&mut self, request: RpcRequest) -> Result<RpcResponse> {
        let id = request.id;
        match self.handle_result(request).await {
            Ok(response) => Ok(response),
            Err(error) => Ok(RpcResponse::error(id, -32000, error.to_string())),
        }
    }

    pub async fn maintenance_tick(
        &mut self,
        last_tick: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let _ = self.runtime.reconcile_exited_tasks().await?;
        let _ = self.runtime.reconcile_watch_changes().await?;
        let _ = self
            .runtime
            .reconcile_memory_limits(crate::runtime::process_memory_bytes)
            .await?;
        let _ = self.runtime.reconcile_health_checks().await?;
        let _ = run_due_actions(&mut self.runtime, last_tick, now).await?;
        Ok(())
    }

    async fn handle_result(&mut self, request: RpcRequest) -> Result<RpcResponse> {
        let id = request.id;
        let result = match request.method.as_str() {
            "task.start" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.start_task(&params.task).await?)?
            }
            "task.stop" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.stop_task(&params.task).await?)?
            }
            "task.restart" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.restart_task(&params.task).await?)?
            }
            "task.reload" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.reload_task(&params.task).await?)?
            }
            "task.start_all" => serde_json::to_value(start_all(&mut self.runtime).await?)?,
            "task.stop_all" => serde_json::to_value(stop_all(&mut self.runtime).await?)?,
            "task.wait" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.wait_task_exit(&params.task).await?)?
            }
            "task.describe" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.describe_task(&params.task)?)?
            }
            "task.logs" => {
                let params = task_params(request.params)?;
                serde_json::to_value(self.runtime.read_task_log(&params.task)?)?
            }
            "task.list" => serde_json::to_value(self.runtime.list_tasks()?)?,
            "event.list" => serde_json::to_value(self.runtime.list_events())?,
            "config.apply" => {
                let params = apply_params(request.params)?;
                let config = ProjectConfig::from_toml_str(&params.toml)?;
                let graph = TaskGraph::from_config(&config)?;
                let _ = graph.plan_all()?;
                self.persist_applied_config(&params.toml)?;
                serde_json::to_value(self.runtime.apply_config(config).await?)?
            }
            other => {
                return Ok(RpcResponse::error(
                    id,
                    -32601,
                    format!("unknown method [{other}]"),
                ));
            }
        };
        Ok(RpcResponse::success(id, result))
    }

    fn persist_applied_config(&self, toml_text: &str) -> Result<()> {
        let Some(path) = &self.applied_config_path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create applied config directory [{}]",
                    parent.display()
                )
            })?;
        }
        std::fs::write(path, toml_text)
            .with_context(|| format!("failed to write applied config [{}]", path.display()))?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct TaskParams {
    task: String,
}

#[derive(Debug, Deserialize)]
struct ApplyParams {
    toml: String,
}

fn task_params(params: serde_json::Value) -> Result<TaskParams> {
    Ok(serde_json::from_value(params)?)
}

fn apply_params(params: serde_json::Value) -> Result<ApplyParams> {
    Ok(serde_json::from_value(params)?)
}
