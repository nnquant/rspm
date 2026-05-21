use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::transport::TcpRspmClient;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonOwnership {
    Detached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonCommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RspmSupervisor {
    addr: SocketAddr,
    binary_path: PathBuf,
    log_dir: PathBuf,
    state_dir: PathBuf,
    socket_path: PathBuf,
    token: Option<String>,
    startup_timeout: Duration,
    ownership: DaemonOwnership,
}

impl Default for RspmSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl RspmSupervisor {
    pub fn new() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 27691)),
            binary_path: std::env::var_os("RSPM_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("rspm")),
            log_dir: PathBuf::from(".rspm/logs"),
            state_dir: PathBuf::from(".rspm/state"),
            socket_path: PathBuf::from(".rspm/run/rspmd.sock"),
            token: None,
            startup_timeout: Duration::from_secs(10),
            ownership: DaemonOwnership::Detached,
        }
    }

    pub fn addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    pub fn binary_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.binary_path = path.into();
        self
    }

    pub fn log_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.log_dir = path.into();
        self
    }

    pub fn state_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.state_dir = path.into();
        self
    }

    pub fn socket_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.socket_path = path.into();
        self
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    pub fn ownership(&self) -> DaemonOwnership {
        self.ownership
    }

    pub fn daemon_command_spec(&self, config: impl AsRef<Path>) -> DaemonCommandSpec {
        let mut args = vec![
            "daemon".to_string(),
            "run".to_string(),
            config.as_ref().display().to_string(),
            self.addr.to_string(),
            self.log_dir.display().to_string(),
            self.state_dir.display().to_string(),
            self.socket_path.display().to_string(),
        ];
        if let Some(token) = &self.token {
            args.push("--token".to_string());
            args.push(token.clone());
        }
        DaemonCommandSpec {
            program: self.binary_path.clone(),
            args,
        }
    }

    pub async fn ensure_daemon(&self, config: impl AsRef<Path>) -> Result<TcpRspmClient> {
        if let Ok(client) = self.connect_ready().await {
            return Ok(client);
        }

        self.ensure_config_source(config.as_ref())?;
        self.spawn_daemon(config.as_ref())?;
        self.wait_ready().await
    }

    fn ensure_config_source(&self, config: &Path) -> Result<()> {
        let applied_config = self.state_dir.join("applied.toml");
        if config.exists() || applied_config.exists() {
            return Ok(());
        }
        anyhow::bail!(
            "missing config [{}] and no applied config [{}]; pass an existing config path before spawning rspmd",
            config.display(),
            applied_config.display()
        );
    }

    fn spawn_daemon(&self, config: &Path) -> Result<()> {
        std::fs::create_dir_all(&self.log_dir).with_context(|| {
            format!(
                "failed to create daemon log directory [{}]",
                self.log_dir.display()
            )
        })?;
        std::fs::create_dir_all(&self.state_dir).with_context(|| {
            format!(
                "failed to create daemon state directory [{}]",
                self.state_dir.display()
            )
        })?;
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create daemon socket directory [{}]",
                    parent.display()
                )
            })?;
        }

        let stdout_path = self.log_dir.join("rspmd.stdout.log");
        let stderr_path = self.log_dir.join("rspmd.stderr.log");
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stdout_path)
            .with_context(|| format!("failed to open daemon stdout [{}]", stdout_path.display()))?;
        let stderr = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stderr_path)
            .with_context(|| format!("failed to open daemon stderr [{}]", stderr_path.display()))?;

        let spec = self.daemon_command_spec(config);
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        detach_daemon_command(&mut command);
        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn rspmd via [{}]", spec.program.display()))?;
        std::fs::write(self.state_dir.join("rspmd.pid"), child.id().to_string()).with_context(
            || {
                format!(
                    "failed to write daemon pid file [{}]",
                    self.state_dir.join("rspmd.pid").display()
                )
            },
        )?;
        Ok(())
    }

    async fn wait_ready(&self) -> Result<TcpRspmClient> {
        let deadline = Instant::now() + self.startup_timeout;
        loop {
            if let Ok(client) = self.connect_ready().await {
                return Ok(client);
            }
            if Instant::now() >= deadline {
                anyhow::bail!("rspmd did not become ready at [{}]", self.addr);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn connect_ready(&self) -> Result<TcpRspmClient> {
        let mut client = TcpRspmClient::connect(self.addr).await?;
        if let Some(token) = &self.token {
            client = client.with_token(token.clone());
        }
        let _ = client.list_tasks().await?;
        Ok(client)
    }
}

#[cfg(unix)]
fn detach_daemon_command(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(any(unix, windows)))]
fn detach_daemon_command(_command: &mut Command) {}

#[cfg(windows)]
fn detach_daemon_command(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;

    command.creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
}
