use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use rspm_core::config::ProjectConfig;
use rspm_core::dag::TaskGraph;
use tokio::sync::Mutex;

use crate::api::DaemonApi;
use crate::orchestrator::start_autostart;
use crate::runtime::TaskRuntime;
#[cfg(windows)]
use crate::server::serve_named_pipe_shared;
use crate::server::serve_tcp_shared;
#[cfg(unix)]
use crate::server::serve_unix_shared;

#[derive(Debug, Clone)]
pub struct DaemonOptions {
    pub config_path: PathBuf,
    pub address: String,
    pub log_dir: PathBuf,
    pub state_dir: PathBuf,
    pub socket_path: PathBuf,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            config_path: PathBuf::from("rspm.toml"),
            address: default_listen_address(),
            log_dir: PathBuf::from(".rspm/logs"),
            state_dir: PathBuf::from(".rspm/state"),
            socket_path: PathBuf::from(".rspm/run/rspmd.sock"),
        }
    }
}

pub async fn run_daemon(options: DaemonOptions) -> Result<()> {
    let applied_config_path = options.state_dir.join("applied.toml");
    let event_log_path = options.state_dir.join("events.jsonl");
    let config_source = if applied_config_path.exists() {
        &applied_config_path
    } else {
        &options.config_path
    };

    let config_text = fs::read_to_string(config_source)
        .with_context(|| format!("failed to read config [{}]", config_source.display()))?;
    let config = ProjectConfig::from_toml_str(&config_text).context("failed to parse config")?;
    let _ = TaskGraph::from_config(&config)?.plan_all()?;
    let mut runtime =
        TaskRuntime::new(config, &options.log_dir)?.with_event_log_path(event_log_path);
    start_autostart_tasks(&mut runtime).await?;

    let api = Arc::new(Mutex::new(
        DaemonApi::new(runtime).with_applied_config_path(applied_config_path),
    ));
    spawn_maintenance_loop(api.clone());
    #[cfg(unix)]
    spawn_unix_socket_server(options.socket_path, api.clone());
    #[cfg(windows)]
    if !options.address.starts_with(r"\\.\pipe\") {
        spawn_named_pipe_server(r"\\.\pipe\rspmd".to_string(), api.clone());
    }

    serve_default_transport(&options.address, api).await
}

pub fn default_listen_address() -> String {
    if cfg!(target_os = "windows") {
        r"\\.\pipe\rspmd".to_string()
    } else {
        "127.0.0.1:27691".to_string()
    }
}

async fn serve_default_transport(address: &str, api: Arc<Mutex<DaemonApi>>) -> Result<()> {
    #[cfg(windows)]
    {
        if address.starts_with(r"\\.\pipe\") {
            return serve_named_pipe_shared(address, api).await;
        }
    }
    serve_tcp_shared(address, api).await
}

async fn start_autostart_tasks(runtime: &mut TaskRuntime) -> Result<()> {
    if runtime.config().tasks.values().any(|task| task.autostart) {
        let _ = start_autostart(runtime).await?;
    }
    Ok(())
}

#[cfg(unix)]
fn spawn_unix_socket_server(path: PathBuf, api: Arc<Mutex<DaemonApi>>) {
    tokio::spawn(async move {
        let _ = serve_unix_shared(&path, api).await;
    });
}

#[cfg(windows)]
fn spawn_named_pipe_server(pipe_name: String, api: Arc<Mutex<DaemonApi>>) {
    tokio::spawn(async move {
        let _ = serve_named_pipe_shared(&pipe_name, api).await;
    });
}

fn spawn_maintenance_loop(api: Arc<Mutex<DaemonApi>>) {
    tokio::spawn(async move {
        let mut last_tick = Utc::now();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let now = Utc::now();
            let _ = api.lock().await.maintenance_tick(last_tick, now).await;
            last_tick = now;
        }
    });
}
