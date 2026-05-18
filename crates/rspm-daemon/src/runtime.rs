use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rspm_core::config::{BackoffMode, ProjectConfig, ReloadMode, RestartPolicy};
use rspm_core::event::{EventType, TaskEvent};
use rspm_core::state::{TaskInfo, TaskStatus};
use tokio::process::Command;

use crate::logs::task_log_path;
use crate::supervisor::ManagedTask;

pub struct TaskRuntime {
    config: ProjectConfig,
    log_dir: PathBuf,
    running: BTreeMap<String, ManagedTask>,
    restart_counts: BTreeMap<String, u32>,
    last_started_at: BTreeMap<String, DateTime<Utc>>,
    last_stopped_at: BTreeMap<String, DateTime<Utc>>,
    last_uptime_ms: BTreeMap<String, u64>,
    last_exit_codes: BTreeMap<String, Option<i32>>,
    health_states: BTreeMap<String, bool>,
    watch_state: BTreeMap<PathBuf, WatchSnapshot>,
    event_log_path: Option<PathBuf>,
    events: Vec<TaskEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WatchSnapshot {
    modified: std::time::SystemTime,
    len: u64,
}

impl TaskRuntime {
    pub fn new(config: ProjectConfig, log_dir: impl AsRef<Path>) -> Result<Self> {
        let log_dir = log_dir.as_ref().to_path_buf();
        fs::create_dir_all(&log_dir)
            .with_context(|| format!("failed to create log dir [{}]", log_dir.display()))?;

        Ok(Self {
            config,
            log_dir,
            running: BTreeMap::new(),
            restart_counts: BTreeMap::new(),
            last_started_at: BTreeMap::new(),
            last_stopped_at: BTreeMap::new(),
            last_uptime_ms: BTreeMap::new(),
            last_exit_codes: BTreeMap::new(),
            health_states: BTreeMap::new(),
            watch_state: BTreeMap::new(),
            event_log_path: None,
            events: Vec::new(),
        })
    }

    pub fn with_event_log_path(mut self, path: impl AsRef<Path>) -> Self {
        self.event_log_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn log_path(&self, task_name: &str) -> PathBuf {
        task_log_path(&self.log_dir, task_name)
    }

    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }

    pub async fn apply_config(&mut self, config: ProjectConfig) -> Result<Vec<TaskInfo>> {
        let removed = self
            .config
            .tasks
            .keys()
            .filter(|task_name| !config.tasks.contains_key(*task_name))
            .cloned()
            .collect::<Vec<_>>();

        for task_name in removed {
            let _ = self.stop_task(&task_name).await?;
            self.restart_counts.remove(&task_name);
            self.last_started_at.remove(&task_name);
            self.last_stopped_at.remove(&task_name);
            self.last_uptime_ms.remove(&task_name);
            self.last_exit_codes.remove(&task_name);
            self.health_states.remove(&task_name);
        }

        self.config = config;
        self.push_project_event(
            EventType::ConfigApplied,
            Some("config.apply".to_string()),
            Some("configuration applied".to_string()),
        );
        self.list_tasks()
    }

    pub async fn start_task(&mut self, task_name: &str) -> Result<TaskInfo> {
        if self.running.contains_key(task_name) {
            return self.describe_task(task_name);
        }

        let task = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        let log_path = self.log_path(task_name);
        if let Some(logs) = &task.logs {
            rotate_log_if_needed(&log_path, logs.max_bytes, logs.max_age_seconds)?;
        }
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("failed to open log file [{}]", log_path.display()))?;
        let stderr = stdout
            .try_clone()
            .with_context(|| format!("failed to clone log file [{}]", log_path.display()))?;

        let mut command = Command::new(&task.cmd);
        command.args(&task.args);
        command.stdout(Stdio::from(stdout));
        command.stderr(Stdio::from(stderr));

        if let Some(cwd) = &task.cwd {
            command.current_dir(cwd);
        }

        for (key, value) in &task.env {
            command.env(key, value);
        }

        let child = command
            .spawn()
            .with_context(|| format!("failed to start task [{task_name}]"))?;
        let restart_count = self.restart_counts.get(task_name).copied().unwrap_or(0);
        let mut managed = ManagedTask::new(child);
        managed.restart_count = restart_count;
        self.last_started_at
            .insert(task_name.to_string(), managed.started_at_utc);
        self.last_stopped_at.remove(task_name);
        self.last_uptime_ms.remove(task_name);
        self.last_exit_codes.remove(task_name);
        self.running.insert(task_name.to_string(), managed);

        let info = self.describe_task(task_name)?;
        self.push_task_event(
            task_name,
            EventType::TaskStarted,
            None,
            Some(TaskStatus::Online),
            Some("start".to_string()),
        );
        Ok(info)
    }

    pub async fn stop_task(&mut self, task_name: &str) -> Result<TaskInfo> {
        let Some(mut managed) = self.running.remove(task_name) else {
            return self.describe_stopped_task(task_name, None);
        };

        managed.status = TaskStatus::Stopping;
        let _ = managed.child.kill().await;
        let status = managed
            .child
            .wait()
            .await
            .with_context(|| format!("failed to wait for task [{task_name}]"))?;
        let exit_code = status.code();
        self.record_task_stop(task_name, &managed, exit_code);
        let info = self.describe_stopped_task(task_name, exit_code)?;
        self.health_states.remove(task_name);
        self.push_task_event(
            task_name,
            EventType::TaskStopped,
            Some(TaskStatus::Stopping),
            Some(TaskStatus::Stopped),
            Some("stop".to_string()),
        );
        Ok(info)
    }

    pub async fn restart_task(&mut self, task_name: &str) -> Result<TaskInfo> {
        let _ = self.stop_task(task_name).await?;
        self.start_task(task_name).await
    }

    pub async fn reload_task(&mut self, task_name: &str) -> Result<TaskInfo> {
        let task = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        let Some(reload) = &task.reload else {
            anyhow::bail!(
                "reload is not configured for task [{}]; configure a reload signal or command",
                task_name
            );
        };

        match reload.mode {
            ReloadMode::Command => {
                let Some(command) = &reload.command else {
                    anyhow::bail!("reload command is missing for task [{}]", task_name);
                };
                let status = if cfg!(target_os = "windows") {
                    Command::new("cmd").args(["/C", command]).status().await?
                } else {
                    Command::new("sh").args(["-c", command]).status().await?
                };
                if !status.success() {
                    anyhow::bail!("reload command failed for task [{}]", task_name);
                }
            }
            ReloadMode::Signal => {
                let Some(signal) = &reload.signal else {
                    anyhow::bail!("reload signal is missing for task [{}]", task_name);
                };
                let Some(pid) = self
                    .running
                    .get(task_name)
                    .and_then(|managed| managed.child.id())
                else {
                    anyhow::bail!("task [{}] is not running", task_name);
                };
                if cfg!(target_os = "windows") {
                    anyhow::bail!(
                        "signal reload is not supported on Windows for task [{}]; use command reload",
                        task_name
                    );
                }
                let status = Command::new("kill")
                    .args(["-s", signal, &pid.to_string()])
                    .status()
                    .await?;
                if !status.success() {
                    anyhow::bail!("reload signal failed for task [{}]", task_name);
                }
            }
        }

        self.push_task_event(
            task_name,
            EventType::TaskRestarted,
            None,
            None,
            Some("reload".to_string()),
        );
        self.describe_task(task_name)
    }

    pub async fn reconcile_exited_tasks(&mut self) -> Result<Vec<TaskInfo>> {
        let mut exited = Vec::new();

        for (task_name, managed) in &mut self.running {
            if let Some(status) = managed
                .child
                .try_wait()
                .with_context(|| format!("failed to poll task [{task_name}]"))?
            {
                exited.push((task_name.clone(), status.code()));
            }
        }

        let mut restarted = Vec::new();
        for (task_name, exit_code) in exited {
            if let Some(managed) = self.running.remove(&task_name) {
                self.record_task_stop(&task_name, &managed, exit_code);
            }
            self.health_states.remove(&task_name);
            self.push_task_event(
                &task_name,
                EventType::TaskStopped,
                Some(TaskStatus::Online),
                Some(TaskStatus::Stopped),
                Some("exit".to_string()),
            );

            if self.should_restart(&task_name, exit_code)? {
                let next_count = self.restart_counts.get(&task_name).copied().unwrap_or(0) + 1;
                self.restart_counts.insert(task_name.clone(), next_count);
                self.push_task_event(
                    &task_name,
                    EventType::TaskRestarted,
                    Some(TaskStatus::Stopped),
                    Some(TaskStatus::Starting),
                    Some("restart_policy".to_string()),
                );
                if let Some(delay) = self.restart_delay(next_count) {
                    tokio::time::sleep(delay).await;
                }
                restarted.push(self.start_task(&task_name).await?);
            }
        }

        Ok(restarted)
    }

    pub fn snapshot_watch_state(&mut self) -> Result<()> {
        for path in self.watch_paths() {
            if let Some(snapshot) = watch_snapshot(&path) {
                self.watch_state.insert(path, snapshot);
            }
        }
        Ok(())
    }

    pub async fn reconcile_watch_changes(&mut self) -> Result<Vec<TaskInfo>> {
        let mut changed_tasks = Vec::new();

        for (task_name, task) in &self.config.tasks {
            let Some(watch) = &task.watch else {
                continue;
            };
            let mut changed = false;
            for path in &watch.paths {
                let path = PathBuf::from(path);
                let Some(snapshot) = watch_snapshot(&path) else {
                    continue;
                };
                let previous = self.watch_state.insert(path, snapshot);
                if previous.is_some_and(|previous| previous != snapshot) {
                    changed = true;
                }
            }
            if changed {
                changed_tasks.push(task_name.clone());
            }
        }

        let mut restarted = Vec::new();
        for task_name in changed_tasks {
            self.push_task_event(
                &task_name,
                EventType::TaskRestarted,
                Some(TaskStatus::Online),
                Some(TaskStatus::Starting),
                Some("watch".to_string()),
            );
            restarted.push(self.restart_task(&task_name).await?);
        }

        Ok(restarted)
    }

    pub async fn reconcile_memory_limits<F>(&mut self, memory_bytes: F) -> Result<Vec<TaskInfo>>
    where
        F: Fn(u32) -> Option<u64>,
    {
        let mut exceeded = Vec::new();
        for (task_name, managed) in &self.running {
            let Some(limit) = self
                .config
                .task(task_name)?
                .limits
                .as_ref()
                .and_then(|limits| limits.max_memory_bytes)
            else {
                continue;
            };
            let Some(pid) = managed.child.id() else {
                continue;
            };
            if memory_bytes(pid).is_some_and(|used| used > limit) {
                exceeded.push(task_name.clone());
            }
        }

        let mut restarted = Vec::new();
        for task_name in exceeded {
            self.push_task_event(
                &task_name,
                EventType::TaskRestarted,
                Some(TaskStatus::Online),
                Some(TaskStatus::Starting),
                Some("memory_limit".to_string()),
            );
            restarted.push(self.restart_task(&task_name).await?);
        }

        Ok(restarted)
    }

    pub async fn reconcile_health_checks(&mut self) -> Result<Vec<TaskInfo>> {
        let mut changed = Vec::new();
        let running_tasks = self.running.keys().cloned().collect::<Vec<_>>();

        for task_name in running_tasks {
            let Some(health) = self
                .config
                .task(&task_name)
                .ok()
                .and_then(|task| task.health.as_ref())
                .cloned()
            else {
                self.health_states.remove(&task_name);
                continue;
            };
            let is_healthy = crate::health::check_health(&health).await?;
            if self.health_states.insert(task_name.clone(), is_healthy) != Some(is_healthy) {
                let status_after = if is_healthy {
                    TaskStatus::Healthy
                } else {
                    TaskStatus::Unhealthy
                };
                self.push_task_event(
                    &task_name,
                    if is_healthy {
                        EventType::TaskHealthy
                    } else {
                        EventType::TaskUnhealthy
                    },
                    Some(TaskStatus::Online),
                    Some(status_after),
                    Some("health_check".to_string()),
                );
                changed.push(self.describe_task(&task_name)?);
            }
        }

        Ok(changed)
    }

    pub fn set_task_health(&mut self, task_name: &str, is_healthy: bool) -> Result<TaskInfo> {
        if self.health_states.insert(task_name.to_string(), is_healthy) != Some(is_healthy) {
            let status_after = if is_healthy {
                TaskStatus::Healthy
            } else {
                TaskStatus::Unhealthy
            };
            self.push_task_event(
                task_name,
                if is_healthy {
                    EventType::TaskHealthy
                } else {
                    EventType::TaskUnhealthy
                },
                Some(TaskStatus::Online),
                Some(status_after),
                Some("health_check".to_string()),
            );
        }
        self.describe_task(task_name)
    }

    pub async fn wait_task_exit(&mut self, task_name: &str) -> Result<TaskInfo> {
        let Some(mut managed) = self.running.remove(task_name) else {
            return self.describe_stopped_task(task_name, None);
        };

        let status = managed
            .child
            .wait()
            .await
            .with_context(|| format!("failed to wait for task [{task_name}]"))?;
        let exit_code = status.code();
        self.record_task_stop(task_name, &managed, exit_code);
        let info = self.describe_stopped_task(task_name, exit_code)?;
        self.health_states.remove(task_name);
        self.push_task_event(
            task_name,
            EventType::TaskStopped,
            Some(TaskStatus::Online),
            Some(TaskStatus::Stopped),
            Some("exit".to_string()),
        );
        Ok(info)
    }

    pub fn describe_task(&self, task_name: &str) -> Result<TaskInfo> {
        let task = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        let dependents = self.dependents(task_name);

        if let Some(managed) = self.running.get(task_name) {
            let uptime_ms = managed.started_at.elapsed().as_millis() as u64;
            let health = self.health_states.get(task_name).copied();
            let status = match health {
                Some(true) => TaskStatus::Healthy,
                Some(false) => TaskStatus::Unhealthy,
                None => managed.status,
            };
            return Ok(TaskInfo {
                task_id: self.task_id(task_name)?,
                name: task_name.to_string(),
                run_mode: self.task_run_mode(task),
                pid: managed.child.id(),
                status,
                health: health.map(|is_healthy| {
                    if is_healthy {
                        "ok".to_string()
                    } else {
                        "fail".to_string()
                    }
                }),
                started_at: Some(managed.started_at_utc),
                stopped_at: None,
                uptime_ms: Some(uptime_ms),
                memory_bytes: managed.child.id().and_then(process_memory_bytes),
                restart_count: self.restart_counts.get(task_name).copied().unwrap_or(0),
                last_exit_code: managed.last_exit_code,
                cwd: task.cwd.clone(),
                cmd: task.cmd.clone(),
                dependencies: task.depends_on.clone(),
                dependents,
                schedule_state: None,
            });
        }

        self.describe_stopped_task(task_name, None)
    }

    pub fn list_tasks(&self) -> Result<Vec<TaskInfo>> {
        self.config
            .tasks
            .keys()
            .map(|task_name| self.describe_task(task_name))
            .collect()
    }

    pub fn read_task_log(&self, task_name: &str) -> Result<String> {
        let _ = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        let path = self.log_path(task_name);
        if !path.exists() {
            return Ok(String::new());
        }
        fs::read_to_string(&path)
            .with_context(|| format!("failed to read log file [{}]", path.display()))
    }

    pub fn list_events(&self) -> Vec<TaskEvent> {
        self.events.clone()
    }

    pub fn record_task_event(
        &mut self,
        task_name: &str,
        event_type: EventType,
        reason: impl Into<String>,
    ) {
        self.push_task_event(task_name, event_type, None, None, Some(reason.into()));
    }

    fn describe_stopped_task(&self, task_name: &str, exit_code: Option<i32>) -> Result<TaskInfo> {
        let task = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;

        Ok(TaskInfo {
            task_id: self.task_id(task_name)?,
            name: task_name.to_string(),
            run_mode: self.task_run_mode(task),
            pid: None,
            status: TaskStatus::Stopped,
            health: None,
            started_at: self.last_started_at.get(task_name).copied(),
            stopped_at: self.last_stopped_at.get(task_name).copied(),
            uptime_ms: self.last_uptime_ms.get(task_name).copied(),
            memory_bytes: None,
            restart_count: self.restart_counts.get(task_name).copied().unwrap_or(0),
            last_exit_code: exit_code
                .or_else(|| self.last_exit_codes.get(task_name).copied().flatten()),
            cwd: task.cwd.clone(),
            cmd: task.cmd.clone(),
            dependencies: task.depends_on.clone(),
            dependents: self.dependents(task_name),
            schedule_state: None,
        })
    }

    fn record_task_stop(&mut self, task_name: &str, managed: &ManagedTask, exit_code: Option<i32>) {
        self.last_started_at
            .insert(task_name.to_string(), managed.started_at_utc);
        self.last_stopped_at
            .insert(task_name.to_string(), Utc::now());
        self.last_uptime_ms.insert(
            task_name.to_string(),
            managed.started_at.elapsed().as_millis() as u64,
        );
        self.last_exit_codes
            .insert(task_name.to_string(), exit_code);
    }

    fn task_run_mode(&self, task: &rspm_core::config::TaskConfig) -> String {
        if task
            .schedule
            .as_ref()
            .is_some_and(|schedule| schedule.start.is_some() || schedule.stop.is_some())
        {
            return "scheduled".to_string();
        }

        if !task.cron.is_empty() {
            return "cron".to_string();
        }

        let restart_policy = task.restart.unwrap_or(self.config.defaults.restart);
        if restart_policy != RestartPolicy::Never || task.health.is_some() || task.watch.is_some() {
            return "long".to_string();
        }

        "oneshot".to_string()
    }

    fn dependents(&self, task_name: &str) -> Vec<String> {
        self.config
            .tasks
            .iter()
            .filter(|(_, task)| {
                task.depends_on
                    .iter()
                    .any(|dependency| dependency == task_name)
            })
            .map(|(candidate, _)| candidate.clone())
            .collect()
    }

    fn task_id(&self, task_name: &str) -> Result<u32> {
        let position = self
            .config
            .tasks
            .keys()
            .position(|candidate| candidate == task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        Ok(position as u32 + 1)
    }

    fn watch_paths(&self) -> Vec<PathBuf> {
        self.config
            .tasks
            .values()
            .filter_map(|task| task.watch.as_ref())
            .flat_map(|watch| watch.paths.iter().map(PathBuf::from))
            .collect()
    }

    fn push_task_event(
        &mut self,
        task_name: &str,
        event_type: EventType,
        status_before: Option<TaskStatus>,
        status_after: Option<TaskStatus>,
        reason: Option<String>,
    ) {
        let mut event = TaskEvent::new(self.config.project.name.clone(), event_type);
        event.task = Some(task_name.to_string());
        event.status_before = status_before;
        event.status_after = status_after;
        event.reason = reason;
        if let Ok(info) = self.describe_task(task_name) {
            event.pid = info.pid;
            event.exit_code = info.last_exit_code;
        }
        self.persist_event(&event);
        self.events.push(event);
    }

    fn push_project_event(
        &mut self,
        event_type: EventType,
        reason: Option<String>,
        message: Option<String>,
    ) {
        let mut event = TaskEvent::new(self.config.project.name.clone(), event_type);
        event.reason = reason;
        event.message = message;
        self.persist_event(&event);
        self.events.push(event);
    }

    fn persist_event(&self, event: &TaskEvent) {
        let Some(path) = &self.event_log_path else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let Ok(line) = serde_json::to_string(event) else {
            return;
        };
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(file, "{line}");
        }
    }

    fn should_restart(&self, task_name: &str, exit_code: Option<i32>) -> Result<bool> {
        let task = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        let policy = task.restart.unwrap_or(self.config.defaults.restart);
        let current_count = self.restart_counts.get(task_name).copied().unwrap_or(0);
        if let Some(max_restarts) = self.config.defaults.max_restarts {
            if current_count >= max_restarts {
                return Ok(false);
            }
        }

        Ok(match policy {
            RestartPolicy::Never => false,
            RestartPolicy::OnFailure => exit_code != Some(0),
            RestartPolicy::Always => true,
        })
    }

    fn restart_delay(&self, restart_count: u32) -> Option<Duration> {
        let base = parse_duration(self.config.defaults.restart_delay.as_deref()?)?;
        let delay = match self.config.defaults.backoff {
            BackoffMode::None => base,
            BackoffMode::Exponential => {
                let multiplier = 2_u32.saturating_pow(restart_count.saturating_sub(1).min(16));
                base.saturating_mul(multiplier)
            }
        };
        self.config
            .defaults
            .max_backoff
            .as_deref()
            .and_then(parse_duration)
            .map(|max_backoff| delay.min(max_backoff))
            .or(Some(delay))
    }
}

pub fn require_task_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(anyhow!("task name cannot be empty"));
    }
    Ok(())
}

fn watch_snapshot(path: &Path) -> Option<WatchSnapshot> {
    let metadata = fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    Some(WatchSnapshot {
        modified,
        len: metadata.len(),
    })
}

fn parse_duration(input: &str) -> Option<Duration> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    if let Some(value) = input.strip_suffix("ms") {
        return value.trim().parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(value) = input.strip_suffix('s') {
        return value.trim().parse::<u64>().ok().map(Duration::from_secs);
    }
    if let Some(value) = input.strip_suffix('m') {
        return value
            .trim()
            .parse::<u64>()
            .ok()
            .map(|minutes| Duration::from_secs(minutes * 60));
    }
    input.parse::<u64>().ok().map(Duration::from_secs)
}

fn rotate_log_if_needed(
    path: &Path,
    max_bytes: Option<u64>,
    max_age_seconds: Option<u64>,
) -> Result<()> {
    let Ok(metadata) = fs::metadata(path) else {
        return Ok(());
    };
    let size_exceeded = max_bytes.is_some_and(|max_bytes| metadata.len() >= max_bytes);
    let age_exceeded = max_age_seconds.is_some_and(|max_age_seconds| {
        metadata
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age.as_secs() >= max_age_seconds)
    });
    if !size_exceeded && !age_exceeded {
        return Ok(());
    }

    let rotated = path.with_extension("log.1");
    if rotated.exists() {
        fs::remove_file(&rotated)
            .with_context(|| format!("failed to remove old rotated log [{}]", rotated.display()))?;
    }
    fs::rename(path, &rotated).with_context(|| {
        format!(
            "failed to rotate log [{}] to [{}]",
            path.display(),
            rotated.display()
        )
    })?;
    Ok(())
}

pub fn process_memory_bytes(pid: u32) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        for line in status.lines() {
            let Some(rest) = line.strip_prefix("VmRSS:") else {
                continue;
            };
            let kb = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())?;
            return Some(kb * 1024);
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}
