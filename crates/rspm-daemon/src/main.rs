use std::path::PathBuf;

use anyhow::Result;
use rspm_daemon::daemon::{default_listen_address, run_daemon, DaemonOptions};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let config_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rspm.toml"));
    let address = args.next().unwrap_or_else(default_listen_address);
    let log_dir = args.next().unwrap_or_else(|| ".rspm/logs".to_string());
    let state_dir = args.next().unwrap_or_else(|| ".rspm/state".to_string());
    let socket_path = args
        .next()
        .unwrap_or_else(|| ".rspm/run/rspmd.sock".to_string());

    run_daemon(DaemonOptions {
        config_path,
        address,
        log_dir: PathBuf::from(log_dir),
        state_dir: PathBuf::from(state_dir),
        socket_path: PathBuf::from(socket_path),
    })
    .await
}
