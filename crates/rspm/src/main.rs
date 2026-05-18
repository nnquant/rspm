use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use rspm_core::api::RpcRequest;
use rspm_core::config::{ProjectConfig, RestartPolicy, TaskConfig};
use rspm_core::dag::TaskGraph;
use rspm_core::event::TaskEvent;
use rspm_core::state::{TaskInfo, TaskStatus};
use rspm_daemon::daemon::{run_daemon, DaemonOptions};
use rspm_sdk::TcpRspmClient;

#[derive(Debug, Parser)]
#[command(name = "rspm")]
#[command(about = "Rust task process manager")]
struct Cli {
    #[arg(long, global = true)]
    addr: Option<SocketAddr>,

    #[arg(long, global = true, default_value = ".rspm/logs")]
    log_dir: PathBuf,

    #[arg(long, global = true, default_value = ".rspm/state")]
    state_dir: PathBuf,

    #[arg(long, global = true, default_value = ".rspm/run/rspmd.sock")]
    socket_path: PathBuf,

    #[arg(long, global = true)]
    no_auto_daemon: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
    },
    Apply {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    Graph {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
        #[arg(long, value_enum, default_value_t = GraphFormat::Text)]
        format: GraphFormat,
    },
    Status {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
    },
    Ls {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
    },
    Monit {
        #[arg(long, default_value_t = 2)]
        interval: u64,
    },
    Start {
        #[arg(required = true)]
        tasks: Vec<String>,
    },
    Stop {
        #[arg(required = true)]
        tasks: Vec<String>,
    },
    Restart {
        #[arg(required = true)]
        tasks: Vec<String>,
    },
    Describe {
        task: String,
    },
    Reload {
        task: String,
    },
    Logs {
        task: String,
        #[arg(short, long)]
        follow: bool,
    },
    Log {
        task: String,
        #[arg(short, long)]
        follow: bool,
        #[arg(long)]
        no_follow: bool,
    },
    Events,
    Doctor {
        #[arg(short, long, default_value = "rspm.toml")]
        config: PathBuf,
        #[arg(long, default_value = ".rspm/logs")]
        log_dir: PathBuf,
    },
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    #[command(hide = true)]
    Daemon {
        #[arg(default_value = "rspm.toml")]
        config: PathBuf,
        #[arg(default_value = "127.0.0.1:27691")]
        listen_addr: String,
        #[arg(default_value = ".rspm/logs")]
        log_dir: PathBuf,
        #[arg(default_value = ".rspm/state")]
        state_dir: PathBuf,
        #[arg(default_value = ".rspm/run/rspmd.sock")]
        socket_path: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum GraphFormat {
    Text,
    Dot,
    Json,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    Install {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        activate: bool,
        #[arg(long, default_value = "rspm.toml")]
        config: PathBuf,
        #[arg(long = "listen", default_value = "127.0.0.1:27691")]
        listen_addr: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Uninstall {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        activate: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let daemon = DaemonLaunch::new(
        cli.addr,
        cli.log_dir.clone(),
        cli.state_dir.clone(),
        cli.socket_path.clone(),
        !cli.no_auto_daemon,
    );

    match cli.command {
        Command::Validate { file } => {
            let config = read_config(&file)?;
            let graph = TaskGraph::from_config(&config)?;
            let _ = graph.plan_all()?;
            println!(
                "valid [{}] tasks=[{}]",
                config.project.name,
                config.tasks.len()
            );
        }
        Command::Apply { file, dry_run } => {
            let config = read_config(&file)?;
            let graph = TaskGraph::from_config(&config)?;
            let plan = graph.plan_all()?;
            if dry_run {
                println!(
                    "apply dry-run [{}] tasks={}",
                    config.project.name,
                    config.tasks.len()
                );
            } else {
                let addr = daemon.ensure(&file).await?;
                let text = fs::read_to_string(&file)
                    .with_context(|| format!("failed to read config [{}]", file.display()))?;
                let tasks = daemon_apply(addr, &text).await?;
                println!("applied [{}] tasks={}", config.project.name, tasks.len());
                for task in &tasks {
                    print_task_result(task);
                }
            }
            println!("start_order={}", plan.start_order.join(","));
            println!("stop_order={}", plan.stop_order.join(","));
        }
        Command::Graph { file, format } => {
            let config = read_config(&file)?;
            let graph = TaskGraph::from_config(&config)?;
            let _ = graph.plan_all()?;
            print_graph(&config, format)?;
        }
        Command::Status { file } | Command::Ls { file } => {
            if daemon.should_use_daemon() {
                let addr = daemon.ensure(&file).await?;
                let tasks = daemon_list(addr).await?;
                print_task_status(&tasks);
            } else {
                let config = read_config(&file)?;
                print_offline_status(&config);
            }
        }
        Command::Monit { interval } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            loop {
                print!("\x1B[2J\x1B[H");
                let tasks = daemon_list(addr).await?;
                print_task_status(&tasks);
                std::io::stdout().flush()?;
                tokio::time::sleep(std::time::Duration::from_secs(interval.max(1))).await;
            }
        }
        Command::Start { tasks } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            if is_all_target(&tasks) {
                let tasks = daemon_all_action(addr, "task.start_all").await?;
                for info in &tasks {
                    print_task_result(info);
                }
            } else {
                for task in resolve_task_targets(addr, &tasks).await? {
                    let info = daemon_task_action(addr, "task.start", &task).await?;
                    print_task_result(&info);
                }
            }
            print_daemon_status(addr).await?;
        }
        Command::Stop { tasks } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            if is_all_target(&tasks) {
                let tasks = daemon_all_action(addr, "task.stop_all").await?;
                for info in &tasks {
                    print_task_result(info);
                }
            } else {
                for task in resolve_task_targets(addr, &tasks).await? {
                    let info = daemon_task_action(addr, "task.stop", &task).await?;
                    print_task_result(&info);
                }
            }
            print_daemon_status(addr).await?;
        }
        Command::Restart { tasks } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            for task in resolve_task_targets(addr, &tasks).await? {
                let info = daemon_task_action(addr, "task.restart", &task).await?;
                print_task_result(&info);
            }
            print_daemon_status(addr).await?;
        }
        Command::Describe { task } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let task = resolve_task_target(addr, &task).await?;
            let info = daemon_task_action(addr, "task.describe", &task).await?;
            print_task_description(&info);
        }
        Command::Reload { task } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let task = resolve_task_target(addr, &task).await?;
            let info = daemon_task_action(addr, "task.reload", &task).await?;
            print_task_result(&info);
            print_daemon_status(addr).await?;
        }
        Command::Logs { task, follow } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let task = resolve_task_target(addr, &task).await?;
            if follow {
                follow_logs(addr, &task).await?;
            } else {
                let logs = daemon_task_logs(addr, &task).await?;
                print_prefixed_logs(&task, &logs)?;
            }
        }
        Command::Log {
            task,
            follow,
            no_follow,
        } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let task = resolve_task_target(addr, &task).await?;
            if follow || !no_follow {
                follow_logs(addr, &task).await?;
            } else {
                let logs = daemon_task_logs(addr, &task).await?;
                print_prefixed_logs(&task, &logs)?;
            }
        }
        Command::Events => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let events = daemon_events(addr).await?;
            print_events(&events);
        }
        Command::Doctor { config, log_dir } => {
            let doctor_daemon = daemon.with_log_dir(log_dir.clone());
            let addr = doctor_daemon.ensure(&config).await?;
            let tasks = daemon_list(addr).await?;
            println!("daemon: ok addr=[{addr}]");
            println!(
                "config: {} path=[{}]",
                if config.exists() { "ok" } else { "missing" },
                config.display()
            );
            println!(
                "log_dir: {} path=[{}]",
                if log_dir.exists() { "ok" } else { "missing" },
                log_dir.display()
            );
            println!("permission: ok cwd-writable=[{}]", cwd_writable());
            println!("tasks: {}", tasks.len());
        }
        Command::Service { command } => match command {
            ServiceCommand::Install {
                dry_run,
                activate,
                config,
                listen_addr,
                output,
            } => {
                let template = service_template(&config, &listen_addr);
                let path = output.unwrap_or_else(default_service_path);
                if dry_run {
                    print!("{template}");
                    if activate {
                        print_activation_commands(&path);
                    }
                } else {
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent).with_context(|| {
                            format!("failed to create service directory [{}]", parent.display())
                        })?;
                    }
                    fs::write(&path, template).with_context(|| {
                        format!("failed to write service file [{}]", path.display())
                    })?;
                    println!("service file written [{}]", path.display());
                    if activate {
                        run_shell_commands(&activation_commands(&path)).await?;
                    }
                }
            }
            ServiceCommand::Uninstall { dry_run, activate } => {
                if dry_run {
                    println!("service uninstall dry-run");
                    if activate {
                        print_deactivation_commands();
                    }
                } else {
                    let path = default_service_path();
                    if activate {
                        run_shell_commands(&deactivation_commands()).await?;
                    }
                    if path.exists() {
                        fs::remove_file(&path).with_context(|| {
                            format!("failed to remove service file [{}]", path.display())
                        })?;
                    }
                    println!("service file removed [{}]", path.display());
                }
            }
        },
        Command::Daemon {
            config,
            listen_addr,
            log_dir,
            state_dir,
            socket_path,
        } => {
            run_daemon(DaemonOptions {
                config_path: config,
                address: listen_addr,
                log_dir,
                state_dir,
                socket_path,
            })
            .await?;
        }
    }

    Ok(())
}

fn read_config(path: &PathBuf) -> Result<ProjectConfig> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read config [{}]", path.display()))?;
    ProjectConfig::from_toml_str(&text).context("failed to load config")
}

fn print_offline_status(config: &ProjectConfig) {
    println!(
        "{:<8} {:<16} {:<10} {:<8} {:<12} {:<8} {:<8} {:<10} {:<15} {:<15} {:<8} {:<8} NEXT",
        "TASK_ID",
        "NAME",
        "MODE",
        "PID",
        "STATUS",
        "HEALTH",
        "RESTARTS",
        "UPTIME",
        "START_TIME",
        "STOP_TIME",
        "CPU",
        "MEM"
    );
    for (index, (task_name, task)) in config.tasks.iter().enumerate() {
        println!(
            "{:<8} {:<16} {:<10} {:<8} {:<12} {:<8} {:<8} {:<10} {:<15} {:<15} {:<8} {:<8} -",
            index + 1,
            task_name,
            config_task_run_mode(config, task),
            "-",
            "stopped",
            "-",
            0,
            "-",
            "-",
            "-",
            "-",
            "-"
        );
    }
}

fn print_task_status(tasks: &[TaskInfo]) {
    println!(
        "{:<8} {:<16} {:<10} {:<8} {:<12} {:<8} {:<8} {:<10} {:<15} {:<15} {:<8} {:<8} NEXT",
        "TASK_ID",
        "NAME",
        "MODE",
        "PID",
        "STATUS",
        "HEALTH",
        "RESTARTS",
        "UPTIME",
        "START_TIME",
        "STOP_TIME",
        "CPU",
        "MEM"
    );
    for task in tasks {
        let status = colored_status_cell(task.status);
        let health = colored_health_cell(task.health.as_deref().unwrap_or("-"));
        let restarts = colored_restarts_cell(task.restart_count);
        let cpu = colored_cpu_cell("-");
        let memory = colored_memory_cell(task.memory_bytes);
        let pid = task
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string());
        let uptime = task
            .uptime_ms
            .map(format_duration)
            .unwrap_or_else(|| "-".to_string());
        let started = task
            .started_at
            .as_ref()
            .map(format_task_time)
            .unwrap_or_else(|| "-".to_string());
        let stopped = task
            .stopped_at
            .as_ref()
            .map(format_task_time)
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<8} {:<16} {:<10} {:<8} {} {} {} {:<10} {:<15} {:<15} {} {} {}",
            task.task_id,
            task.name,
            display_run_mode(task),
            pid,
            status,
            health,
            restarts,
            uptime,
            started,
            stopped,
            cpu,
            memory,
            task.schedule_state.as_deref().unwrap_or("-")
        );
    }
}

fn print_task_result(task: &TaskInfo) {
    println!(
        "task_id={} {} {} pid={}",
        task.task_id,
        task.name,
        status_label(task.status),
        task.pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

fn print_task_description(task: &TaskInfo) {
    println!("task_id: {}", task.task_id);
    println!("name: {}", task.name);
    println!(
        "pid: {}",
        task.pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("status: {}", colored_status_label(task.status));
    println!("mode: {}", display_run_mode(task));
    println!(
        "start_time: {}",
        task.started_at
            .as_ref()
            .map(DateTime::<Utc>::to_rfc3339)
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "stop_time: {}",
        task.stopped_at
            .as_ref()
            .map(DateTime::<Utc>::to_rfc3339)
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "uptime: {}",
        task.uptime_ms
            .map(format_duration)
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "health: {}",
        colored_health_label(task.health.as_deref().unwrap_or("-"))
    );
    println!(
        "memory: {}",
        task.memory_bytes
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string())
    );
    println!("restarts: {}", task.restart_count);
    println!("cmd: {}", task.cmd);
    println!("cwd: {}", task.cwd.as_deref().unwrap_or("-"));
    println!(
        "dependencies: {}",
        if task.dependencies.is_empty() {
            "-".to_string()
        } else {
            task.dependencies.join(",")
        }
    );
    println!(
        "dependents: {}",
        if task.dependents.is_empty() {
            "-".to_string()
        } else {
            task.dependents.join(",")
        }
    );
}

fn print_graph(config: &ProjectConfig, format: GraphFormat) -> Result<()> {
    match format {
        GraphFormat::Text => {
            for (task_name, task) in &config.tasks {
                if task.depends_on.is_empty() {
                    println!("{task_name}");
                }
                for dependency in &task.depends_on {
                    println!("{dependency} -> {task_name}");
                }
            }
        }
        GraphFormat::Dot => {
            println!("digraph rspm {{");
            for (task_name, task) in &config.tasks {
                if task.depends_on.is_empty() {
                    println!("  \"{task_name}\";");
                }
                for dependency in &task.depends_on {
                    println!("  \"{dependency}\" -> \"{task_name}\";");
                }
            }
            println!("}}");
        }
        GraphFormat::Json => {
            let edges = config
                .tasks
                .iter()
                .flat_map(|(task_name, task)| {
                    task.depends_on.iter().map(move |dependency| {
                        serde_json::json!({
                            "from": dependency,
                            "to": task_name,
                        })
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "project": &config.project.name,
                    "tasks": config.tasks.keys().collect::<Vec<_>>(),
                    "edges": edges,
                }))?
            );
        }
    }
    Ok(())
}

fn is_all_target(tasks: &[String]) -> bool {
    tasks.len() == 1 && tasks[0] == "all"
}

async fn print_daemon_status(addr: SocketAddr) -> Result<()> {
    let tasks = daemon_list(addr).await?;
    print_task_status(&tasks);
    Ok(())
}

async fn resolve_task_target(addr: SocketAddr, target: &str) -> Result<String> {
    let targets = [target.to_string()];
    let mut resolved = resolve_task_targets(addr, &targets).await?;
    Ok(resolved.remove(0))
}

async fn resolve_task_targets(addr: SocketAddr, targets: &[String]) -> Result<Vec<String>> {
    let mut resolved = Vec::with_capacity(targets.len());
    let mut task_list = None;

    for target in targets {
        if let Ok(task_id) = target.parse::<u32>() {
            let tasks = match &task_list {
                Some(tasks) => tasks,
                None => task_list.insert(daemon_list(addr).await?),
            };
            let Some(task) = tasks.iter().find(|task| task.task_id == task_id) else {
                anyhow::bail!("task_id [{task_id}] not found");
            };
            resolved.push(task.name.clone());
        } else {
            resolved.push(target.clone());
        }
    }

    Ok(resolved)
}

#[derive(Debug, Clone)]
struct DaemonLaunch {
    addr: SocketAddr,
    explicit_addr: bool,
    log_dir: PathBuf,
    state_dir: PathBuf,
    socket_path: PathBuf,
    auto_launch: bool,
}

impl DaemonLaunch {
    fn new(
        addr: Option<SocketAddr>,
        log_dir: PathBuf,
        state_dir: PathBuf,
        socket_path: PathBuf,
        auto_launch: bool,
    ) -> Self {
        Self {
            addr: addr.unwrap_or_else(default_daemon_addr),
            explicit_addr: addr.is_some(),
            log_dir,
            state_dir,
            socket_path,
            auto_launch,
        }
    }

    fn should_use_daemon(&self) -> bool {
        self.auto_launch || self.explicit_addr
    }

    fn with_log_dir(&self, log_dir: PathBuf) -> Self {
        let mut next = self.clone();
        next.log_dir = log_dir;
        next
    }

    async fn ensure(&self, config: &Path) -> Result<SocketAddr> {
        if !self.auto_launch {
            return Ok(self.addr);
        }
        if probe_daemon(self.addr).await.is_ok() {
            return Ok(self.addr);
        }
        self.spawn(config)?;
        wait_for_daemon(self.addr).await?;
        Ok(self.addr)
    }

    fn spawn(&self, config: &Path) -> Result<()> {
        fs::create_dir_all(&self.log_dir).with_context(|| {
            format!(
                "failed to create daemon log directory [{}]",
                self.log_dir.display()
            )
        })?;
        fs::create_dir_all(&self.state_dir).with_context(|| {
            format!(
                "failed to create daemon state directory [{}]",
                self.state_dir.display()
            )
        })?;
        if let Some(parent) = self.socket_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create daemon socket directory [{}]",
                    parent.display()
                )
            })?;
        }

        let stdout_path = self.log_dir.join("rspmd.stdout.log");
        let stderr_path = self.log_dir.join("rspmd.stderr.log");
        let stdout = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stdout_path)
            .with_context(|| format!("failed to open daemon stdout [{}]", stdout_path.display()))?;
        let stderr = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stderr_path)
            .with_context(|| format!("failed to open daemon stderr [{}]", stderr_path.display()))?;

        let exe = std::env::current_exe().context("failed to locate rspm executable")?;
        let child = tokio::process::Command::new(exe)
            .arg("daemon")
            .arg(config)
            .arg(self.addr.to_string())
            .arg(&self.log_dir)
            .arg(&self.state_dir)
            .arg(&self.socket_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("failed to spawn rspmd through rspm daemon")?;

        if let Some(pid) = child.id() {
            fs::write(self.state_dir.join("rspmd.pid"), pid.to_string()).with_context(|| {
                format!(
                    "failed to write daemon pid file [{}]",
                    self.state_dir.join("rspmd.pid").display()
                )
            })?;
        }
        Ok(())
    }
}

async fn wait_for_daemon(addr: SocketAddr) -> Result<()> {
    let mut last_error = None;
    for _ in 0..100 {
        match probe_daemon(addr).await {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    if let Some(error) = last_error {
        anyhow::bail!("rspmd did not become ready at [{addr}]: {error}");
    }
    anyhow::bail!("rspmd did not become ready at [{addr}]");
}

async fn probe_daemon(addr: SocketAddr) -> Result<()> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd probe failed [{}]: {}", error.code, error.message);
    }
    response.result.context("rspmd probe returned no result")?;
    Ok(())
}

async fn daemon_list(addr: SocketAddr) -> Result<Vec<TaskInfo>> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
        .await?;
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn follow_logs(addr: SocketAddr, task: &str) -> Result<()> {
    let mut printed = 0;
    loop {
        let logs = daemon_task_logs(addr, task).await?;
        if logs.len() < printed {
            printed = 0;
        }
        if logs.len() > printed {
            print_prefixed_logs(task, &logs[printed..])?;
            printed = logs.len();
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

fn print_prefixed_logs(task: &str, logs: &str) -> Result<()> {
    for line in logs.split_inclusive('\n') {
        print!("{task} | {line}");
    }
    if !logs.is_empty() && !logs.ends_with('\n') {
        println!();
    }
    std::io::stdout().flush()?;
    Ok(())
}

async fn daemon_task_action(addr: SocketAddr, method: &str, task: &str) -> Result<TaskInfo> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(
            1,
            method,
            serde_json::json!({ "task": task }),
        ))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn daemon_task_logs(addr: SocketAddr, task: &str) -> Result<String> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(
            1,
            "task.logs",
            serde_json::json!({ "task": task }),
        ))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn daemon_all_action(addr: SocketAddr, method: &str) -> Result<Vec<TaskInfo>> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(1, method, serde_json::json!({})))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn daemon_events(addr: SocketAddr) -> Result<Vec<TaskEvent>> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(1, "event.list", serde_json::json!({})))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn daemon_apply(addr: SocketAddr, toml_text: &str) -> Result<Vec<TaskInfo>> {
    let mut client = TcpRspmClient::connect(addr).await?;
    let response = client
        .send(RpcRequest::new(
            1,
            "config.apply",
            serde_json::json!({ "toml": toml_text }),
        ))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

fn print_events(events: &[TaskEvent]) {
    println!("{:<26} {:<16} {:<18} REASON", "TIMESTAMP", "TASK", "EVENT");
    for event in events {
        println!(
            "{:<26} {:<16} {:<18} {}",
            event.timestamp.to_rfc3339(),
            event.task.as_deref().unwrap_or("-"),
            event_label(&event.event_type),
            event.reason.as_deref().unwrap_or("-")
        );
    }
}

fn event_label(event_type: &rspm_core::event::EventType) -> &'static str {
    match event_type {
        rspm_core::event::EventType::TaskStarted => "task_started",
        rspm_core::event::EventType::TaskHealthy => "task_healthy",
        rspm_core::event::EventType::TaskUnhealthy => "task_unhealthy",
        rspm_core::event::EventType::TaskExited => "task_exited",
        rspm_core::event::EventType::TaskRestarted => "task_restarted",
        rspm_core::event::EventType::TaskStopped => "task_stopped",
        rspm_core::event::EventType::DependencyWaiting => "dependency_waiting",
        rspm_core::event::EventType::ScheduleTriggered => "schedule_triggered",
        rspm_core::event::EventType::CronTriggered => "cron_triggered",
        rspm_core::event::EventType::ConfigApplied => "config_applied",
    }
}

fn default_daemon_addr() -> SocketAddr {
    "127.0.0.1:27691"
        .parse()
        .expect("default daemon address must be valid")
}

fn status_label(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Defined => "defined",
        TaskStatus::Scheduled => "scheduled",
        TaskStatus::WaitingDependency => "waiting",
        TaskStatus::Starting => "starting",
        TaskStatus::Online => "online",
        TaskStatus::Healthy => "healthy",
        TaskStatus::Unhealthy => "unhealthy",
        TaskStatus::Stopping => "stopping",
        TaskStatus::Stopped => "stopped",
        TaskStatus::Failed => "failed",
        TaskStatus::Backoff => "backoff",
        TaskStatus::Disabled => "disabled",
    }
}

fn colored_status_cell(status: TaskStatus) -> String {
    colorize_status(status, &format!("{:<12}", status_label(status)))
}

fn colored_status_label(status: TaskStatus) -> String {
    colorize_status(status, status_label(status))
}

fn colorize_status(status: TaskStatus, value: &str) -> String {
    let color = match status {
        TaskStatus::Online | TaskStatus::Healthy => "32",
        TaskStatus::Unhealthy | TaskStatus::Failed | TaskStatus::Stopped => "31",
        TaskStatus::Starting
        | TaskStatus::Scheduled
        | TaskStatus::WaitingDependency
        | TaskStatus::Stopping => "33",
        TaskStatus::Backoff => "35",
        TaskStatus::Disabled | TaskStatus::Defined => "90",
    };
    format!("\x1b[{color}m{value}\x1b[0m")
}

fn colored_health_cell(health: &str) -> String {
    colorize_health(health, &format!("{health:<8}"))
}

fn colored_health_label(health: &str) -> String {
    colorize_health(health, health)
}

fn colorize_health(health: &str, value: &str) -> String {
    match health {
        "ok" => format!("\x1b[32m{value}\x1b[0m"),
        "fail" => format!("\x1b[31m{value}\x1b[0m"),
        "-" => value.to_string(),
        _ => format!("\x1b[33m{value}\x1b[0m"),
    }
}

fn colored_restarts_cell(restart_count: u32) -> String {
    let value = format!("{restart_count:<8}");
    if restart_count >= 3 {
        format!("\x1b[33m{value}\x1b[0m")
    } else {
        value
    }
}

fn colored_cpu_cell(cpu: &str) -> String {
    let value = format!("{cpu:<8}");
    if parse_cpu_percent(cpu).is_some_and(|percent| percent >= 80.0) {
        format!("\x1b[33m{value}\x1b[0m")
    } else {
        value
    }
}

fn colored_memory_cell(memory_bytes: Option<u64>) -> String {
    let value = memory_bytes
        .map(format_bytes)
        .unwrap_or_else(|| "-".to_string());
    let value = format!("{value:<8}");
    if memory_bytes.is_some_and(|bytes| bytes >= 512 * 1024 * 1024) {
        format!("\x1b[33m{value}\x1b[0m")
    } else {
        value
    }
}

fn parse_cpu_percent(cpu: &str) -> Option<f64> {
    let value = cpu.trim().strip_suffix('%').unwrap_or(cpu.trim());
    value.parse::<f64>().ok()
}

fn config_task_run_mode(config: &ProjectConfig, task: &TaskConfig) -> &'static str {
    if task
        .schedule
        .as_ref()
        .is_some_and(|schedule| schedule.start.is_some() || schedule.stop.is_some())
    {
        return "scheduled";
    }

    if !task.cron.is_empty() {
        return "cron";
    }

    let restart_policy = task.restart.unwrap_or(config.defaults.restart);
    if restart_policy != RestartPolicy::Never || task.health.is_some() || task.watch.is_some() {
        return "long";
    }

    "oneshot"
}

fn display_run_mode(task: &TaskInfo) -> &str {
    if task.run_mode.is_empty() {
        "-"
    } else {
        &task.run_mode
    }
}

fn format_task_time(time: &DateTime<Utc>) -> String {
    time.format("%m-%d %H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_formats_seconds_minutes_hours_and_days() {
        assert_eq!(format_duration(999), "0s");
        assert_eq!(format_duration(1_000), "1s");
        assert_eq!(format_duration(61_000), "1m1s");
        assert_eq!(format_duration(3_661_000), "1h1m");
        assert_eq!(format_duration(90_061_000), "1d1h");
    }

    #[test]
    fn stopped_status_is_red() {
        assert!(colored_status_cell(TaskStatus::Stopped).starts_with("\x1b[31m"));
    }

    #[test]
    fn high_restart_count_is_yellow() {
        assert_eq!(colored_restarts_cell(3), "\x1b[33m3       \x1b[0m");
    }

    #[test]
    fn high_cpu_and_memory_cells_are_yellow() {
        assert_eq!(colored_cpu_cell("80%"), "\x1b[33m80%     \x1b[0m");
        assert_eq!(
            colored_memory_cell(Some(512 * 1024 * 1024)),
            "\x1b[33m512MB   \x1b[0m"
        );
    }
}

fn format_duration(uptime_ms: u64) -> String {
    let seconds = uptime_ms / 1_000;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        let seconds = seconds % 60;
        return format!("{minutes}m{seconds}s");
    }
    let hours = minutes / 60;
    if hours < 24 {
        let minutes = minutes % 60;
        return format!("{hours}h{minutes}m");
    }
    let days = hours / 24;
    let hours = hours % 24;
    format!("{days}d{hours}h")
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        return format!("{}GB", bytes / GB);
    }
    if bytes >= MB {
        return format!("{}MB", bytes / MB);
    }
    if bytes >= KB {
        return format!("{}KB", bytes / KB);
    }
    format!("{bytes}B")
}

fn service_template(config: &Path, addr: &str) -> String {
    let config = config.display();
    let state_dir = ".rspm/state";
    if cfg!(target_os = "windows") {
        return format!(
            r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>rspm daemon</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled></LogonTrigger></Triggers>
  <Principals><Principal id="Author"><LogonType>InteractiveToken</LogonType></Principal></Principals>
  <Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><Enabled>true</Enabled></Settings>
  <Actions Context="Author">
    <Exec>
      <Command>rspm.exe</Command>
      <Arguments>daemon {config} {addr} .rspm/logs {state_dir}</Arguments>
    </Exec>
  </Actions>
</Task>
"#
        );
    }
    if cfg!(target_os = "macos") {
        return format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>rspmd</string>
  <key>ProgramArguments</key>
  <array>
    <string>rspm</string>
    <string>daemon</string>
    <string>{config}</string>
    <string>{addr}</string>
    <string>.rspm/logs</string>
    <string>{state_dir}</string>
  </array>
  <key>RunAtLoad</key><true/>
</dict>
</plist>
"#
        );
    }
    format!(
        r#"[Unit]
Description=rspm daemon

[Service]
ExecStart=rspm daemon {config} {addr} .rspm/logs {state_dir}
Restart=always

[Install]
WantedBy=default.target
"#
    )
}

fn default_service_path() -> PathBuf {
    if cfg!(target_os = "windows") {
        let base = std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        return base.join("rspm").join("rspmd-task.xml");
    }
    if cfg!(target_os = "macos") {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        return home
            .join("Library")
            .join("LaunchAgents")
            .join("io.rspm.rspmd.plist");
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".config")
        .join("systemd")
        .join("user")
        .join("rspmd.service")
}

fn activation_commands(path: &Path) -> Vec<String> {
    if cfg!(target_os = "windows") {
        return vec![format!(
            "schtasks /Create /TN rspmd /XML {} /F",
            shell_quote(path)
        )];
    } else if cfg!(target_os = "macos") {
        return vec![
            format!("launchctl bootstrap gui/$(id -u) {}", shell_quote(path)),
            "launchctl enable gui/$(id -u)/rspmd".to_string(),
        ];
    }
    vec![
        "systemctl --user daemon-reload".to_string(),
        "systemctl --user enable --now rspmd.service".to_string(),
    ]
}

fn deactivation_commands() -> Vec<String> {
    if cfg!(target_os = "windows") {
        return vec!["schtasks /Delete /TN rspmd /F".to_string()];
    } else if cfg!(target_os = "macos") {
        return vec![
            "launchctl disable gui/$(id -u)/rspmd".to_string(),
            "launchctl bootout gui/$(id -u)/rspmd".to_string(),
        ];
    }
    vec![
        "systemctl --user disable --now rspmd.service".to_string(),
        "systemctl --user daemon-reload".to_string(),
    ]
}

fn print_activation_commands(path: &Path) {
    for command in activation_commands(path) {
        println!("activation command: {command}");
    }
}

fn print_deactivation_commands() {
    for command in deactivation_commands() {
        println!("deactivation command: {command}");
    }
}

async fn run_shell_commands(commands: &[String]) -> Result<()> {
    for command in commands {
        let status = if cfg!(target_os = "windows") {
            tokio::process::Command::new("cmd")
                .args(["/C", command])
                .status()
                .await?
        } else {
            tokio::process::Command::new("sh")
                .args(["-c", command])
                .status()
                .await?
        };
        if !status.success() {
            anyhow::bail!("service command failed: {command}");
        }
        println!("service command ok: {command}");
    }
    Ok(())
}

fn shell_quote(path: &Path) -> String {
    let value = path.display().to_string();
    if cfg!(target_os = "windows") {
        format!("\"{}\"", value.replace('"', "\\\""))
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn cwd_writable() -> bool {
    let path = PathBuf::from(".rspm-doctor-write-test");
    match fs::write(&path, b"ok") {
        Ok(()) => {
            let _ = fs::remove_file(path);
            true
        }
        Err(_) => false,
    }
}
