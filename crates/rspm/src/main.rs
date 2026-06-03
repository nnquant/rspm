use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use rspm_core::api::RpcRequest;
use rspm_core::config::{ProjectConfig, ProjectSection, TaskConfig};
use rspm_core::dag::TaskGraph;
use rspm_core::display::format_display_time;
use rspm_core::event::TaskEvent;
use rspm_core::state::{TaskInfo, TaskStatus};
use rspm_daemon::daemon::{run_daemon, DaemonOptions};
use rspm_sdk::render::{
    colored_health_label, colored_status_label, format_bytes, format_duration,
    format_merged_logs as render_merged_logs, format_offline_task_table,
    format_prefixed_logs as render_prefixed_logs, format_task_table, status_label,
    RenderLogOptions,
};
use rspm_sdk::TcpRspmClient;

#[derive(Debug, Parser)]
#[command(name = "rspm")]
#[command(about = "Rust task process manager")]
#[command(version)]
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
    token: Option<String>,

    #[arg(long = "no-daemon", alias = "no-auto-daemon", global = true)]
    no_daemon: bool,

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
    Add {
        command: String,
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long, value_name = "KEY[=VALUE]", allow_hyphen_values = true)]
        env: Vec<String>,
        #[arg(long)]
        no_apply: bool,
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
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
        #[arg(long, default_value_t = 2)]
        interval: u64,
        #[arg(long)]
        once: bool,
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
        task: Option<String>,
        #[arg(short, long)]
        follow: bool,
        #[arg(long)]
        no_history: bool,
        #[arg(long)]
        lines: Option<usize>,
        #[arg(long)]
        grep: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        merge: bool,
    },
    Log {
        task: Option<String>,
        #[arg(short, long)]
        follow: bool,
        #[arg(long)]
        no_follow: bool,
        #[arg(long)]
        no_history: bool,
        #[arg(long)]
        lines: Option<usize>,
        #[arg(long)]
        grep: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        merge: bool,
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
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Start {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
    },
    Stop,
    Status,
    Restart {
        #[arg(short, long, default_value = "rspm.toml")]
        file: PathBuf,
    },
    #[command(hide = true)]
    Run {
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
        #[arg(long)]
        token: Option<String>,
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
    Status {
        #[arg(long)]
        dry_run: bool,
    },
    Start {
        #[arg(long)]
        dry_run: bool,
    },
    Stop {
        #[arg(long)]
        dry_run: bool,
    },
    Restart {
        #[arg(long)]
        dry_run: bool,
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
        cli.token.or_else(|| std::env::var("RSPM_TOKEN").ok()),
        !cli.no_daemon,
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
                let tasks = daemon_apply(addr, &text, daemon.token()).await?;
                println!("applied [{}] tasks={}", config.project.name, tasks.len());
                for task in &tasks {
                    print_task_result(task);
                }
            }
            println!("start_order={}", plan.start_order.join(","));
            println!("stop_order={}", plan.stop_order.join(","));
        }
        Command::Add {
            command,
            file,
            name,
            cwd,
            env,
            no_apply,
        } => {
            let (config, task_name, task) = add_task_to_config(&file, &command, name, cwd, env)?;
            write_config(&file, &config)?;
            println!(
                "added [{}] cmd=[{}] args=[{}]",
                task_name,
                task.cmd,
                task.args.join(" ")
            );
            if !no_apply && daemon.should_use_daemon() {
                let addr = daemon.ensure(&file).await?;
                let text = fs::read_to_string(&file)
                    .with_context(|| format!("failed to read config [{}]", file.display()))?;
                let tasks = daemon_apply(addr, &text, daemon.token()).await?;
                println!("applied [{}] tasks={}", config.project.name, tasks.len());
                print_task_status(&tasks);
            }
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
                let tasks = daemon_list(addr, daemon.token()).await?;
                print_task_status(&tasks);
            } else {
                let config = read_config(&file)?;
                print_offline_status(&config);
            }
        }
        Command::Monit {
            file,
            interval,
            once,
        } => {
            let addr = daemon.ensure(&file).await?;
            if once {
                let tasks = daemon_list(addr, daemon.token()).await?;
                print_monit_snapshot(addr, &tasks);
                return Ok(());
            }
            loop {
                print!("\x1B[2J\x1B[H");
                let tasks = daemon_list(addr, daemon.token()).await?;
                print_monit_snapshot(addr, &tasks);
                std::io::stdout().flush()?;
                tokio::time::sleep(std::time::Duration::from_secs(interval.max(1))).await;
            }
        }
        Command::Start { tasks } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            if is_all_target(&tasks) {
                let tasks = daemon_all_action(addr, "task.start_all", daemon.token()).await?;
                for info in &tasks {
                    print_task_result(info);
                }
            } else {
                for task in resolve_task_targets(addr, &tasks, daemon.token()).await? {
                    let info =
                        daemon_task_action(addr, "task.start", &task, daemon.token()).await?;
                    print_task_result(&info);
                }
            }
            print_daemon_status(addr, daemon.token()).await?;
        }
        Command::Stop { tasks } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            if is_all_target(&tasks) {
                let tasks = daemon_all_action(addr, "task.stop_all", daemon.token()).await?;
                for info in &tasks {
                    print_task_result(info);
                }
            } else {
                for task in resolve_task_targets(addr, &tasks, daemon.token()).await? {
                    let info = daemon_task_action(addr, "task.stop", &task, daemon.token()).await?;
                    print_task_result(&info);
                }
            }
            print_daemon_status(addr, daemon.token()).await?;
        }
        Command::Restart { tasks } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            for task in resolve_task_targets(addr, &tasks, daemon.token()).await? {
                let info = daemon_task_action(addr, "task.restart", &task, daemon.token()).await?;
                print_task_result(&info);
            }
            print_daemon_status(addr, daemon.token()).await?;
        }
        Command::Describe { task } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let task = resolve_task_target(addr, &task, daemon.token()).await?;
            let info = daemon_task_action(addr, "task.describe", &task, daemon.token()).await?;
            print_task_description(&info);
        }
        Command::Reload { task } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let task = resolve_task_target(addr, &task, daemon.token()).await?;
            let info = daemon_task_action(addr, "task.reload", &task, daemon.token()).await?;
            print_task_result(&info);
            print_daemon_status(addr, daemon.token()).await?;
        }
        Command::Logs {
            task,
            follow,
            no_history,
            lines,
            grep,
            since,
            merge,
        } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let tasks = resolve_log_targets(addr, task.as_deref(), daemon.token()).await?;
            let since = parse_since_timestamp(since.as_deref())?;
            let options = LogPrintOptions {
                lines,
                grep: grep.as_deref(),
                since,
                merge,
            };
            if follow {
                follow_logs(addr, &tasks, !no_history, options, daemon.token()).await?;
            } else {
                print_task_logs(addr, &tasks, options, daemon.token()).await?;
            }
        }
        Command::Log {
            task,
            follow,
            no_follow,
            no_history,
            lines,
            grep,
            since,
            merge,
        } => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let tasks = resolve_log_targets(addr, task.as_deref(), daemon.token()).await?;
            let since = parse_since_timestamp(since.as_deref())?;
            let options = LogPrintOptions {
                lines,
                grep: grep.as_deref(),
                since,
                merge,
            };
            if follow || !no_follow {
                follow_logs(addr, &tasks, !no_history, options, daemon.token()).await?;
            } else {
                print_task_logs(addr, &tasks, options, daemon.token()).await?;
            }
        }
        Command::Events => {
            let addr = daemon.ensure(&PathBuf::from("rspm.toml")).await?;
            let tasks = daemon_list(addr, daemon.token()).await?;
            let display_timezone = tasks
                .iter()
                .find_map(|task| task.display_timezone.as_deref());
            let events = daemon_events(addr, daemon.token()).await?;
            print_events(&events, display_timezone);
        }
        Command::Doctor { config, log_dir } => {
            let doctor_daemon = daemon.with_log_dir(log_dir.clone());
            let addr = doctor_daemon.ensure(&config).await?;
            let tasks = daemon_list(addr, daemon.token()).await?;
            println!("daemon: ok addr=[{addr}]");
            println!(
                "platform: os=[{}] arch=[{}]",
                std::env::consts::OS,
                std::env::consts::ARCH
            );
            println!("default_addr: {}", default_daemon_addr());
            println!(
                "auth_token: {}",
                if doctor_daemon.token().is_some() {
                    "enabled"
                } else {
                    "disabled"
                }
            );
            println!("service_status_command: {}", status_commands().join(" && "));
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
            println!(
                "state_dir: {} path=[{}]",
                if doctor_daemon.state_dir.exists() {
                    "ok"
                } else {
                    "missing"
                },
                doctor_daemon.state_dir.display()
            );
            println!(
                "pid_file: {} path=[{}]",
                pid_file_state(&doctor_daemon.state_dir.join("rspmd.pid")),
                doctor_daemon.state_dir.join("rspmd.pid").display()
            );
            println!(
                "applied_config: {} path=[{}]",
                if doctor_daemon.state_dir.join("applied.toml").exists() {
                    "ok"
                } else {
                    "missing"
                },
                doctor_daemon.state_dir.join("applied.toml").display()
            );
            println!(
                "event_log: {} path=[{}]",
                event_log_state(&doctor_daemon.state_dir.join("events.jsonl")),
                doctor_daemon.state_dir.join("events.jsonl").display()
            );
            println!(
                "socket_path: path=[{}]",
                doctor_daemon.socket_path.display()
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
            ServiceCommand::Status { dry_run } => {
                let path = default_service_path();
                println!(
                    "service_file: {} path=[{}]",
                    if path.exists() { "ok" } else { "missing" },
                    path.display()
                );
                if dry_run {
                    print_status_commands();
                } else {
                    run_shell_commands(&status_commands()).await?;
                }
            }
            ServiceCommand::Start { dry_run } => {
                if dry_run {
                    print_service_commands("start", &start_commands());
                } else {
                    run_shell_commands(&start_commands()).await?;
                }
            }
            ServiceCommand::Stop { dry_run } => {
                if dry_run {
                    print_service_commands("stop", &stop_commands());
                } else {
                    run_shell_commands(&stop_commands()).await?;
                }
            }
            ServiceCommand::Restart { dry_run } => {
                if dry_run {
                    print_service_commands("restart", &restart_commands());
                } else {
                    run_shell_commands(&restart_commands()).await?;
                }
            }
        },
        Command::Daemon { command } => match command {
            DaemonCommand::Start { file } => {
                daemon.start(&file).await?;
            }
            DaemonCommand::Stop => {
                daemon.stop().await?;
            }
            DaemonCommand::Status => {
                daemon.status().await?;
            }
            DaemonCommand::Restart { file } => {
                daemon.restart(&file).await?;
            }
            DaemonCommand::Run {
                config,
                listen_addr,
                log_dir,
                state_dir,
                socket_path,
                token,
            } => {
                run_daemon(DaemonOptions {
                    config_path: config,
                    address: listen_addr,
                    log_dir,
                    state_dir,
                    socket_path,
                    auth_token: token,
                })
                .await?;
            }
        },
    }

    Ok(())
}

fn read_config(path: &PathBuf) -> Result<ProjectConfig> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read config [{}]", path.display()))?;
    ProjectConfig::from_toml_str(&text).context("failed to load config")
}

fn write_config(path: &Path, config: &ProjectConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config directory [{}]", parent.display()))?;
    }
    let text = toml::to_string_pretty(config).context("failed to serialize config")?;
    fs::write(path, text).with_context(|| format!("failed to write config [{}]", path.display()))
}

fn add_task_to_config(
    path: &Path,
    command: &str,
    name: Option<String>,
    cwd: Option<String>,
    env: Vec<String>,
) -> Result<(ProjectConfig, String, TaskConfig)> {
    let mut config = if path.exists() {
        read_config(&path.to_path_buf())?
    } else {
        empty_project_config()
    };
    let parts = parse_command_line(command)?;
    let (cmd, args) = parts
        .split_first()
        .map(|(cmd, args)| (cmd.clone(), args.to_vec()))
        .context("command cannot be empty")?;
    let task_name = unique_task_name(
        name.unwrap_or_else(|| infer_task_name(&cmd, &args)),
        &config.tasks,
    );
    let task = TaskConfig {
        cmd,
        args,
        cwd,
        autostart: false,
        env: parse_env_overrides(env)?,
        restart: None,
        depends_on: Vec::new(),
        start_when: Default::default(),
        health: None,
        schedule: None,
        watch: None,
        limits: None,
        logs: None,
        reload: None,
        cron: BTreeMap::new(),
    };
    config.tasks.insert(task_name.clone(), task.clone());
    config.validate().context("generated config is invalid")?;
    Ok((config, task_name, task))
}

fn parse_env_overrides(values: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut env = BTreeMap::new();
    for value in values {
        let normalized = value.trim_start_matches("--");
        if normalized.is_empty() {
            anyhow::bail!("env key cannot be empty");
        }
        let (key, env_value) = if let Some((key, explicit_value)) = normalized.split_once('=') {
            (key.to_string(), explicit_value.to_string())
        } else {
            (
                normalized.to_string(),
                std::env::var(normalized).unwrap_or_default(),
            )
        };
        validate_env_key(&key)?;
        env.insert(key, env_value);
    }
    Ok(env)
}

fn validate_env_key(key: &str) -> Result<()> {
    let valid = key
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        && key
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_');
    if valid {
        Ok(())
    } else {
        anyhow::bail!("invalid env key [{key}]")
    }
}

fn empty_project_config() -> ProjectConfig {
    ProjectConfig {
        project: ProjectSection::default(),
        defaults: Default::default(),
        tasks: BTreeMap::new(),
    }
}

fn parse_command_line(command: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;

    for ch in command.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => current.push(ch),
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            None => current.push(ch),
        }
    }

    if escaped {
        current.push('\\');
    }
    if let Some(quote) = quote {
        anyhow::bail!("unterminated quote [{quote}] in command [{command}]");
    }
    if !current.is_empty() {
        parts.push(current);
    }
    if parts.is_empty() {
        anyhow::bail!("command cannot be empty");
    }
    Ok(parts)
}

fn infer_task_name(cmd: &str, args: &[String]) -> String {
    let candidate = args
        .iter()
        .rev()
        .find(|arg| arg.ends_with(".py") || arg.ends_with(".rs") || arg.contains('/'))
        .map(String::as_str)
        .unwrap_or(cmd);
    sanitize_task_name(
        Path::new(candidate)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(candidate),
    )
}

fn sanitize_task_name(value: &str) -> String {
    let mut name = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    name = name.trim_matches('_').to_string();
    if name.is_empty() {
        "task".to_string()
    } else {
        name
    }
}

fn unique_task_name(base: String, tasks: &BTreeMap<String, TaskConfig>) -> String {
    if !tasks.contains_key(&base) {
        return base;
    }
    for index in 2.. {
        let candidate = format!("{base}-{index}");
        if !tasks.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!("unbounded task name suffix search should always return")
}

fn print_offline_status(config: &ProjectConfig) {
    print!("{}", format_offline_task_table(config));
}

fn print_task_status(tasks: &[TaskInfo]) {
    print!("{}", format_task_table(tasks));
}

fn print_monit_snapshot(addr: SocketAddr, tasks: &[TaskInfo]) {
    let running = tasks.iter().filter(|task| task.pid.is_some()).count();
    let unhealthy = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Unhealthy)
        .count();
    println!(
        "MONIT addr=[{}] tasks={} running={} unhealthy={}",
        addr,
        tasks.len(),
        running,
        unhealthy
    );
    print_task_status(tasks);
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
            .map(|time| format_task_time(time, task.display_timezone.as_deref()))
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "stop_time: {}",
        task.stopped_at
            .as_ref()
            .map(|time| format_task_time(time, task.display_timezone.as_deref()))
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

async fn print_daemon_status(addr: SocketAddr, token: Option<&str>) -> Result<()> {
    let tasks = daemon_list(addr, token).await?;
    print_task_status(&tasks);
    Ok(())
}

async fn resolve_task_target(
    addr: SocketAddr,
    target: &str,
    token: Option<&str>,
) -> Result<String> {
    let targets = [target.to_string()];
    let mut resolved = resolve_task_targets(addr, &targets, token).await?;
    Ok(resolved.remove(0))
}

async fn resolve_task_targets(
    addr: SocketAddr,
    targets: &[String],
    token: Option<&str>,
) -> Result<Vec<String>> {
    let mut resolved = Vec::with_capacity(targets.len());
    let mut task_list = None;

    for target in targets {
        if let Ok(task_id) = target.parse::<u32>() {
            let tasks = match &task_list {
                Some(tasks) => tasks,
                None => task_list.insert(daemon_list(addr, token).await?),
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

async fn resolve_log_targets(
    addr: SocketAddr,
    target: Option<&str>,
    token: Option<&str>,
) -> Result<Vec<String>> {
    match target {
        Some("all") | None => Ok(daemon_list(addr, token)
            .await?
            .into_iter()
            .map(|task| task.name)
            .collect()),
        Some(target) => Ok(vec![resolve_task_target(addr, target, token).await?]),
    }
}

#[derive(Debug, Clone)]
struct DaemonLaunch {
    addr: SocketAddr,
    explicit_addr: bool,
    log_dir: PathBuf,
    state_dir: PathBuf,
    socket_path: PathBuf,
    auth_token: Option<String>,
    auto_launch: bool,
}

impl DaemonLaunch {
    fn new(
        addr: Option<SocketAddr>,
        log_dir: PathBuf,
        state_dir: PathBuf,
        socket_path: PathBuf,
        auth_token: Option<String>,
        auto_launch: bool,
    ) -> Self {
        Self {
            addr: addr.unwrap_or_else(default_daemon_addr),
            explicit_addr: addr.is_some(),
            log_dir,
            state_dir,
            socket_path,
            auth_token,
            auto_launch,
        }
    }

    fn token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }

    fn should_use_daemon(&self) -> bool {
        self.auto_launch || self.explicit_addr
    }

    fn with_log_dir(&self, log_dir: PathBuf) -> Self {
        let mut next = self.clone();
        next.log_dir = log_dir;
        next
    }

    async fn start(&self, config: &Path) -> Result<()> {
        if probe_daemon(self.addr, self.token()).await.is_ok() {
            println!("daemon already running addr=[{}]", self.addr);
            return Ok(());
        }
        self.spawn(config)?;
        wait_for_daemon(self.addr, self.token()).await?;
        let pid = fs::read_to_string(self.state_dir.join("rspmd.pid"))
            .ok()
            .map(|pid| pid.trim().to_string())
            .filter(|pid| !pid.is_empty())
            .unwrap_or_else(|| "-".to_string());
        println!("daemon started addr=[{}] pid=[{}]", self.addr, pid);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let was_running = probe_daemon(self.addr, self.token()).await.is_ok();
        if was_running {
            let _ = daemon_all_action(self.addr, "task.stop_all", self.token()).await?;
        }

        let pid_path = self.state_dir.join("rspmd.pid");
        let Some(pid) = read_pid_file(&pid_path)? else {
            if was_running {
                anyhow::bail!(
                    "daemon is reachable at [{}] but pid file is missing [{}]",
                    self.addr,
                    pid_path.display()
                );
            }
            println!("daemon not running addr=[{}]", self.addr);
            return Ok(());
        };

        if !was_running {
            let _ = fs::remove_file(&pid_path);
            println!("daemon not running addr=[{}]", self.addr);
            return Ok(());
        }

        terminate_process(pid).await?;
        wait_for_daemon_exit(self.addr, self.token()).await?;
        let _ = fs::remove_file(&pid_path);
        println!("daemon stopped addr=[{}] pid=[{}]", self.addr, pid);
        Ok(())
    }

    async fn restart(&self, config: &Path) -> Result<()> {
        let running_tasks = if probe_daemon(self.addr, self.token()).await.is_ok() {
            daemon_list(self.addr, self.token())
                .await?
                .into_iter()
                .filter(|task| task.pid.is_some())
                .map(|task| task.name)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        self.stop().await?;
        self.start(config).await?;

        for task in running_tasks {
            let info = daemon_task_action(self.addr, "task.start", &task, self.token()).await?;
            print_task_result(&info);
        }
        Ok(())
    }

    async fn status(&self) -> Result<()> {
        let pid_path = self.state_dir.join("rspmd.pid");
        let pid = read_pid_file(&pid_path)?;
        if probe_daemon(self.addr, self.token()).await.is_ok() {
            println!(
                "daemon: running addr=[{}] pid=[{}]",
                self.addr,
                pid.map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
        } else {
            println!(
                "daemon: stopped addr=[{}] pid_file=[{}]",
                self.addr,
                if pid.is_some() { "stale" } else { "missing" }
            );
        }
        Ok(())
    }

    async fn ensure(&self, config: &Path) -> Result<SocketAddr> {
        if !self.auto_launch {
            return Ok(self.addr);
        }
        match probe_daemon(self.addr, self.token()).await {
            Ok(_) => return Ok(self.addr),
            Err(error) => {
                if !wait_for_tcp_addr_available(self.addr).await {
                    anyhow::bail!(
                        "rspmd is not responding at [{}], and the address is still in use: {error}",
                        self.addr
                    );
                }
            }
        }
        self.spawn(config)?;
        wait_for_daemon(self.addr, self.token()).await?;
        Ok(self.addr)
    }

    fn spawn(&self, config: &Path) -> Result<()> {
        self.ensure_config_source(config)?;
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
        let mut command = StdCommand::new(exe);
        command
            .arg("daemon")
            .arg("run")
            .arg(config)
            .arg(self.addr.to_string())
            .arg(&self.log_dir)
            .arg(&self.state_dir)
            .arg(&self.socket_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if let Some(token) = &self.auth_token {
            command.arg("--token").arg(token);
        }
        detach_daemon_command(&mut command);
        let child = command
            .spawn()
            .context("failed to spawn rspmd through rspm daemon")?;

        fs::write(self.state_dir.join("rspmd.pid"), child.id().to_string()).with_context(|| {
            format!(
                "failed to write daemon pid file [{}]",
                self.state_dir.join("rspmd.pid").display()
            )
        })?;
        Ok(())
    }

    fn ensure_config_source(&self, config: &Path) -> Result<()> {
        let applied_config = self.state_dir.join("applied.toml");
        if config.exists() || applied_config.exists() {
            return Ok(());
        }
        anyhow::bail!(
            "missing config [{}] and no applied config [{}]; run `rspm apply -f <file>` or pass `-f <file>`",
            config.display(),
            applied_config.display()
        );
    }
}

#[cfg(unix)]
fn detach_daemon_command(command: &mut StdCommand) {
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

#[cfg(windows)]
fn detach_daemon_command(command: &mut StdCommand) {
    use std::os::windows::process::CommandExt;

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn detach_daemon_command(_command: &mut StdCommand) {}

async fn wait_for_daemon(addr: SocketAddr, token: Option<&str>) -> Result<()> {
    let mut last_error = None;
    for _ in 0..100 {
        match probe_daemon(addr, token).await {
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

async fn wait_for_daemon_exit(addr: SocketAddr, token: Option<&str>) -> Result<()> {
    let _ = token;
    for _ in 0..300 {
        if !tcp_port_open(addr).await {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    anyhow::bail!("rspmd did not stop at [{addr}]");
}

async fn wait_for_tcp_addr_available(addr: SocketAddr) -> bool {
    for _ in 0..20 {
        if std::net::TcpListener::bind(addr).is_ok() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    false
}

async fn tcp_port_open(addr: SocketAddr) -> bool {
    tokio::time::timeout(
        std::time::Duration::from_millis(50),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

async fn probe_daemon(addr: SocketAddr, token: Option<&str>) -> Result<()> {
    let response = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        let mut client = tcp_client(addr, token).await?;
        client
            .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
            .await
    })
    .await
    .context("rspmd probe timed out")??;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd probe failed [{}]: {}", error.code, error.message);
    }
    response.result.context("rspmd probe returned no result")?;
    Ok(())
}

fn read_pid_file(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read daemon pid file [{}]", path.display()))?;
    let pid = text
        .trim()
        .parse::<u32>()
        .with_context(|| format!("invalid daemon pid file [{}]", path.display()))?;
    Ok(Some(pid))
}

async fn terminate_process(pid: u32) -> Result<()> {
    if cfg!(target_os = "windows") {
        let status = tokio::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .status()
            .await
            .context("failed to run taskkill")?;
        if !status.success() {
            anyhow::bail!("failed to stop daemon pid=[{}] with taskkill", pid);
        }
        return Ok(());
    }

    let status = tokio::process::Command::new("kill")
        .arg(pid.to_string())
        .status()
        .await
        .context("failed to run kill")?;
    if !status.success() {
        anyhow::bail!("failed to stop daemon pid=[{}] with kill", pid);
    }
    Ok(())
}

async fn tcp_client(addr: SocketAddr, token: Option<&str>) -> Result<TcpRspmClient> {
    let client = TcpRspmClient::connect(addr).await?;
    Ok(match token {
        Some(token) => client.with_token(token),
        None => client,
    })
}

async fn daemon_list(addr: SocketAddr, token: Option<&str>) -> Result<Vec<TaskInfo>> {
    let mut client = tcp_client(addr, token).await?;
    let response = client
        .send(RpcRequest::new(1, "task.list", serde_json::json!({})))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

#[derive(Debug, Clone, Copy)]
struct LogPrintOptions<'a> {
    lines: Option<usize>,
    grep: Option<&'a str>,
    since: Option<DateTime<Utc>>,
    merge: bool,
}

impl From<LogPrintOptions<'_>> for RenderLogOptions {
    fn from(value: LogPrintOptions<'_>) -> Self {
        Self {
            lines: value.lines,
            grep: value.grep.map(ToString::to_string),
            since: value.since,
        }
    }
}

async fn print_task_logs(
    addr: SocketAddr,
    tasks: &[String],
    options: LogPrintOptions<'_>,
    token: Option<&str>,
) -> Result<()> {
    if options.merge {
        let logs = read_task_logs(addr, tasks, token).await?;
        print_merged_logs(&logs, options)?;
        return Ok(());
    }
    for task in tasks {
        let logs = daemon_task_logs(addr, task, token).await?;
        print_prefixed_logs(task, &logs, options)?;
    }
    Ok(())
}

async fn read_task_logs(
    addr: SocketAddr,
    tasks: &[String],
    token: Option<&str>,
) -> Result<Vec<(String, String)>> {
    let mut result = Vec::with_capacity(tasks.len());
    for task in tasks {
        result.push((task.clone(), daemon_task_logs(addr, task, token).await?));
    }
    Ok(result)
}

fn initial_log_offsets(logs: &[(String, String)], include_history: bool) -> Vec<usize> {
    if include_history {
        vec![0; logs.len()]
    } else {
        logs.iter().map(|(_, log)| log.len()).collect()
    }
}

async fn follow_logs(
    addr: SocketAddr,
    tasks: &[String],
    include_history: bool,
    options: LogPrintOptions<'_>,
    token: Option<&str>,
) -> Result<()> {
    let initial_logs = read_task_logs(addr, tasks, token).await?;
    let mut printed = initial_log_offsets(&initial_logs, include_history);
    if include_history {
        if options.merge {
            print_merged_logs(&initial_logs, options)?;
            for ((_, logs), printed_len) in initial_logs.iter().zip(printed.iter_mut()) {
                *printed_len = logs.len();
            }
        } else {
            for ((task, logs), printed_len) in initial_logs.iter().zip(printed.iter_mut()) {
                if !logs.is_empty() {
                    print_prefixed_logs(task, logs, options)?;
                    *printed_len = logs.len();
                }
            }
        }
    }

    loop {
        let mut updated_logs = Vec::new();
        for (index, task) in tasks.iter().enumerate() {
            let logs = daemon_task_logs(addr, task, token).await?;
            if logs.len() < printed[index] {
                printed[index] = 0;
            }
            if logs.len() > printed[index] {
                let updated = logs[printed[index]..].to_string();
                if options.merge {
                    updated_logs.push((task.clone(), updated));
                } else {
                    print_prefixed_logs(task, &updated, options)?;
                }
                printed[index] = logs.len();
            }
        }
        if options.merge && !updated_logs.is_empty() {
            print_merged_logs(&updated_logs, options)?;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

fn print_prefixed_logs(task: &str, logs: &str, options: LogPrintOptions<'_>) -> Result<()> {
    print!("{}", render_prefixed_logs(task, logs, &options.into()));
    std::io::stdout().flush()?;
    Ok(())
}

fn print_merged_logs(logs: &[(String, String)], options: LogPrintOptions<'_>) -> Result<()> {
    print!("{}", render_merged_logs(logs, &options.into()));
    std::io::stdout().flush()?;
    Ok(())
}

fn parse_since_timestamp(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(Some(
        DateTime::parse_from_rfc3339(value)
            .with_context(|| format!("invalid --since timestamp [{value}]"))?
            .with_timezone(&Utc),
    ))
}

async fn daemon_task_action(
    addr: SocketAddr,
    method: &str,
    task: &str,
    token: Option<&str>,
) -> Result<TaskInfo> {
    let mut client = tcp_client(addr, token).await?;
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

async fn daemon_task_logs(addr: SocketAddr, task: &str, token: Option<&str>) -> Result<String> {
    let mut client = tcp_client(addr, token).await?;
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

async fn daemon_all_action(
    addr: SocketAddr,
    method: &str,
    token: Option<&str>,
) -> Result<Vec<TaskInfo>> {
    let mut client = tcp_client(addr, token).await?;
    let response = client
        .send(RpcRequest::new(1, method, serde_json::json!({})))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn daemon_events(addr: SocketAddr, token: Option<&str>) -> Result<Vec<TaskEvent>> {
    let mut client = tcp_client(addr, token).await?;
    let response = client
        .send(RpcRequest::new(1, "event.list", serde_json::json!({})))
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("rspmd error [{}]: {}", error.code, error.message);
    }
    let result = response.result.context("rspmd returned no result")?;
    Ok(serde_json::from_value(result)?)
}

async fn daemon_apply(
    addr: SocketAddr,
    toml_text: &str,
    token: Option<&str>,
) -> Result<Vec<TaskInfo>> {
    let mut client = tcp_client(addr, token).await?;
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

fn print_events(events: &[TaskEvent], display_timezone: Option<&str>) {
    println!("{:<26} {:<16} {:<18} REASON", "TIMESTAMP", "TASK", "EVENT");
    for event in events {
        println!(
            "{:<26} {:<16} {:<18} {}",
            format_task_time(&event.timestamp, display_timezone),
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

fn display_run_mode(task: &TaskInfo) -> &str {
    if task.run_mode.is_empty() {
        "-"
    } else {
        &task.run_mode
    }
}

fn format_task_time(time: &DateTime<Utc>, display_timezone: Option<&str>) -> String {
    format_display_time(time, display_timezone)
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
    fn command_line_parser_handles_quotes() {
        assert_eq!(
            parse_command_line(r#"uv run "a script.py" --flag value"#).unwrap(),
            vec!["uv", "run", "a script.py", "--flag", "value"]
        );
    }

    #[test]
    fn task_name_prefers_python_script_stem() {
        assert_eq!(
            infer_task_name("uv", &["run".to_string(), "scripts/a.py".to_string()]),
            "a"
        );
    }

    #[test]
    fn env_overrides_copy_current_env_or_accept_inline_values() {
        std::env::set_var("RSPM_TEST_ENV1", "one");
        let env = parse_env_overrides(vec![
            "RSPM_TEST_ENV1".to_string(),
            "--RSPM_TEST_ENV2=two".to_string(),
        ])
        .unwrap();

        assert_eq!(env.get("RSPM_TEST_ENV1").map(String::as_str), Some("one"));
        assert_eq!(env.get("RSPM_TEST_ENV2").map(String::as_str), Some("two"));
    }

    #[test]
    fn follow_offsets_include_or_skip_history() {
        let tasks = vec![
            ("alpha".to_string(), "old-alpha\n".to_string()),
            ("beta".to_string(), "old-beta\n".to_string()),
        ];

        assert_eq!(initial_log_offsets(&tasks, true), vec![0, 0]);
        assert_eq!(initial_log_offsets(&tasks, false), vec![10, 9]);
    }
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
      <Arguments>daemon run {config} {addr} .rspm/logs {state_dir}</Arguments>
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
    <string>run</string>
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
ExecStart=rspm daemon run {config} {addr} .rspm/logs {state_dir}
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

fn status_commands() -> Vec<String> {
    if cfg!(target_os = "windows") {
        return vec!["schtasks /Query /TN rspmd /V /FO LIST".to_string()];
    } else if cfg!(target_os = "macos") {
        return vec!["launchctl print gui/$(id -u)/rspmd".to_string()];
    }
    vec!["systemctl --user status rspmd.service".to_string()]
}

fn start_commands() -> Vec<String> {
    if cfg!(target_os = "windows") {
        return vec!["schtasks /Run /TN rspmd".to_string()];
    } else if cfg!(target_os = "macos") {
        return vec!["launchctl kickstart gui/$(id -u)/rspmd".to_string()];
    }
    vec!["systemctl --user start rspmd.service".to_string()]
}

fn stop_commands() -> Vec<String> {
    if cfg!(target_os = "windows") {
        return vec!["schtasks /End /TN rspmd".to_string()];
    } else if cfg!(target_os = "macos") {
        return vec!["launchctl kill TERM gui/$(id -u)/rspmd".to_string()];
    }
    vec!["systemctl --user stop rspmd.service".to_string()]
}

fn restart_commands() -> Vec<String> {
    if cfg!(target_os = "windows") {
        return vec![
            "schtasks /End /TN rspmd".to_string(),
            "schtasks /Run /TN rspmd".to_string(),
        ];
    } else if cfg!(target_os = "macos") {
        return vec!["launchctl kickstart -k gui/$(id -u)/rspmd".to_string()];
    }
    vec!["systemctl --user restart rspmd.service".to_string()]
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

fn print_status_commands() {
    for command in status_commands() {
        println!("status command: {command}");
    }
}

fn print_service_commands(action: &str, commands: &[String]) {
    for command in commands {
        println!("{action} command: {command}");
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

fn pid_file_state(path: &Path) -> &'static str {
    match read_pid_file(path) {
        Ok(Some(pid)) if process_exists(pid) => "ok",
        Ok(Some(_)) => "stale",
        Ok(None) => "missing",
        Err(_) => "invalid",
    }
}

fn event_log_state(path: &Path) -> &'static str {
    if !path.exists() {
        return "missing";
    }
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() > 0 => "ok",
        Ok(_) => "empty",
        Err(_) => "unreadable",
    }
}

fn process_exists(pid: u32) -> bool {
    if cfg!(target_os = "windows") {
        return StdCommand::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .is_ok_and(|output| {
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
            });
    }

    StdCommand::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}
