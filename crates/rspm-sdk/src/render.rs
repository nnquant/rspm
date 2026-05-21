use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use rspm_core::config::{ProjectConfig, RestartPolicy, TaskConfig};
use rspm_core::display::format_display_table_time;
use rspm_core::state::{TaskInfo, TaskStatus};

const TASK_NAME_WIDTH: usize = 32;

#[derive(Debug, Clone, Default)]
pub struct RenderLogOptions {
    pub lines: Option<usize>,
    pub grep: Option<String>,
    pub since: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct LogLine {
    task: String,
    line: String,
    timestamp: Option<DateTime<Utc>>,
    sequence: usize,
}

pub fn format_task_table(tasks: &[TaskInfo]) -> String {
    let mut output = String::new();
    write_task_table(&mut output, tasks).expect("writing to String cannot fail");
    output
}

pub fn write_task_table(output: &mut impl std::fmt::Write, tasks: &[TaskInfo]) -> std::fmt::Result {
    writeln!(
        output,
        "{:<8} {:<32} {:<10} {:<8} {:<12} {:<8} {:<8} {:<10} {:<15} {:<15} {:<8} {:<8} NEXT",
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
    )?;
    for task in tasks {
        let status = colored_status_cell(task.status);
        let health = colored_health_cell(task.health.as_deref().unwrap_or("-"));
        let restarts = colored_restarts_cell(task.restart_count);
        let cpu_text = task.cpu_percent.map(format_cpu_percent);
        let cpu = colored_cpu_cell(cpu_text.as_deref().unwrap_or("-"));
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
            .map(|time| format_display_table_time(time, task.display_timezone.as_deref()))
            .unwrap_or_else(|| "-".to_string());
        let stopped = task
            .stopped_at
            .as_ref()
            .map(|time| format_display_table_time(time, task.display_timezone.as_deref()))
            .unwrap_or_else(|| "-".to_string());
        writeln!(
            output,
            "{:<8} {:<32} {:<10} {:<8} {} {} {} {:<10} {:<15} {:<15} {} {} {}",
            task.task_id,
            fixed_width_cell(&task.name, TASK_NAME_WIDTH),
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
        )?;
    }
    writeln!(
        output,
        "{}",
        colored_note_line(&format!("Timezone: {}", table_display_timezone(tasks)))
    )
}

pub fn format_offline_task_table(config: &ProjectConfig) -> String {
    let mut output = String::new();
    write_offline_task_table(&mut output, config).expect("writing to String cannot fail");
    output
}

pub fn write_offline_task_table(
    output: &mut impl std::fmt::Write,
    config: &ProjectConfig,
) -> std::fmt::Result {
    writeln!(
        output,
        "{:<8} {:<32} {:<10} {:<8} {:<12} {:<8} {:<8} {:<10} {:<15} {:<15} {:<8} {:<8} NEXT",
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
    )?;
    for (index, (task_name, task)) in config.tasks.iter().enumerate() {
        writeln!(
            output,
            "{:<8} {:<32} {:<10} {:<8} {:<12} {:<8} {:<8} {:<10} {:<15} {:<15} {:<8} {:<8} -",
            index + 1,
            fixed_width_cell(task_name, TASK_NAME_WIDTH),
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
        )?;
    }
    writeln!(
        output,
        "{}",
        colored_note_line(&format!("Timezone: {}", config.project.display_timezone))
    )
}

pub fn format_prefixed_logs(task: &str, logs: &str, options: &RenderLogOptions) -> String {
    let mut output = String::new();
    for line in selected_log_lines(logs, options) {
        let _ = write!(output, "{task} | {line}");
    }
    if !logs.is_empty() && !logs.ends_with('\n') {
        output.push('\n');
    }
    output
}

pub fn format_merged_logs(logs: &[(String, String)], options: &RenderLogOptions) -> String {
    let mut lines = Vec::new();
    let mut sequence = 0_usize;
    for (task, log) in logs {
        for line in selected_log_lines(log, options) {
            lines.push(LogLine {
                task: task.clone(),
                timestamp: line_timestamp(&line),
                line,
                sequence,
            });
            sequence += 1;
        }
    }
    lines.sort_by(|left, right| match (left.timestamp, right.timestamp) {
        (Some(left_ts), Some(right_ts)) => left_ts
            .cmp(&right_ts)
            .then_with(|| left.sequence.cmp(&right.sequence)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.sequence.cmp(&right.sequence),
    });

    let mut output = String::new();
    for line in lines {
        let _ = write!(output, "{} | {}", line.task, line.line);
    }
    output
}

pub fn selected_log_lines(logs: &str, options: &RenderLogOptions) -> Vec<String> {
    let mut selected = logs
        .split_inclusive('\n')
        .filter(|line| options.grep.as_ref().is_none_or(|grep| line.contains(grep)))
        .filter(|line| {
            options.since.is_none_or(|since| {
                line_timestamp(line).is_some_and(|timestamp| timestamp >= since)
            })
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if let Some(lines) = options.lines {
        selected = selected
            .into_iter()
            .rev()
            .take(lines)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
    }
    selected
}

pub fn line_timestamp(line: &str) -> Option<DateTime<Utc>> {
    line.split_whitespace().find_map(|token| {
        let token = token.trim_matches(|ch: char| {
            !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | ':' | '.' | '+' | 'Z' | 'T'))
        });
        DateTime::parse_from_rfc3339(token)
            .ok()
            .map(|time| time.with_timezone(&Utc))
    })
}

pub fn status_label(status: TaskStatus) -> &'static str {
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

pub fn colored_status_label(status: TaskStatus) -> String {
    colorize_status(status, status_label(status))
}

pub fn colored_health_label(health: &str) -> String {
    colorize_health(health, health)
}

fn colored_status_cell(status: TaskStatus) -> String {
    colorize_status(status, &format!("{:<12}", status_label(status)))
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

fn format_cpu_percent(cpu_percent: f64) -> String {
    format!("{cpu_percent:.1}%")
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

pub fn colored_note_line(value: &str) -> String {
    format!("\x1b[90m{value}\x1b[0m")
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

fn table_display_timezone(tasks: &[TaskInfo]) -> &str {
    tasks
        .iter()
        .find_map(|task| task.display_timezone.as_deref())
        .unwrap_or("local")
}

pub fn format_duration(uptime_ms: u64) -> String {
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

pub fn format_bytes(bytes: u64) -> String {
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

fn fixed_width_cell(value: &str, width: usize) -> String {
    let text = if value.chars().count() > width {
        let prefix = value
            .chars()
            .take(width.saturating_sub(3))
            .collect::<String>();
        format!("{prefix}...")
    } else {
        value.to_string()
    };
    format!("{text:<width$}")
}
