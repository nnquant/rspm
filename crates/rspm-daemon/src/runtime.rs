use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rspm_core::config::{BackoffMode, ProjectConfig, ReloadMode, RestartPolicy};
use rspm_core::display::format_display_table_time;
use rspm_core::event::{EventType, TaskEvent};
use rspm_core::schedule::{next_scheduled_action, ScheduledAction, ScheduledActionKind};
use rspm_core::state::{TaskInfo, TaskStatus};
use tokio::process::Command;

use crate::logs::task_log_path;
use crate::supervisor::ManagedTask;

pub struct TaskRuntime {
    config: ProjectConfig,
    log_dir: PathBuf,
    running: BTreeMap<String, ManagedTask>,
    restored_pids: BTreeMap<String, u32>,
    restored_stop_requests: BTreeMap<String, RestoredStopRequest>,
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

#[derive(Debug, Clone, Copy)]
struct RestoredStopRequest {
    status: TaskStatus,
    requested_at: Instant,
    force_kill_sent: bool,
    pending_restart: bool,
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
            restored_pids: BTreeMap::new(),
            restored_stop_requests: BTreeMap::new(),
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
        self.restore_events_from_log();
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
        if self.restored_pid_if_alive(task_name).is_some() {
            return self.describe_task(task_name);
        }
        self.restored_pids.remove(task_name);

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
        let Some(managed) = self.running.get_mut(task_name) else {
            if let Some(pid) = self.restored_pids.get(task_name).copied() {
                if let Some(request) = self.restored_stop_requests.get(task_name) {
                    return self.describe_task(task_name).map(|mut info| {
                        info.status = request.status;
                        info
                    });
                }
                if !is_process_alive(pid) {
                    self.restored_pids.remove(task_name);
                    self.record_restored_task_stop(task_name, None);
                    let info = self.describe_stopped_task(task_name, None, Utc::now())?;
                    self.health_states.remove(task_name);
                    self.push_task_event(
                        task_name,
                        EventType::TaskStopped,
                        Some(TaskStatus::Online),
                        Some(TaskStatus::Stopped),
                        Some("stop".to_string()),
                    );
                    return Ok(info);
                }
                send_term_to_pid(pid)
                    .await
                    .with_context(|| format!("failed to stop restored task [{task_name}]"))?;
                self.restored_stop_requests.insert(
                    task_name.to_string(),
                    RestoredStopRequest {
                        status: TaskStatus::Stopping,
                        requested_at: Instant::now(),
                        force_kill_sent: false,
                        pending_restart: false,
                    },
                );
                self.health_states.remove(task_name);
                self.push_task_event(
                    task_name,
                    EventType::TaskStopped,
                    Some(TaskStatus::Online),
                    Some(TaskStatus::Stopping),
                    Some("stop".to_string()),
                );
                return self.describe_task(task_name);
            }
            return self.describe_stopped_task(task_name, None, Utc::now());
        };

        if matches!(
            managed.status,
            TaskStatus::Stopping | TaskStatus::Restarting
        ) {
            return self.describe_task(task_name);
        }

        let status_before = managed.status;
        managed.status = TaskStatus::Stopping;
        managed.stop_requested_at = Some(Instant::now());
        managed.force_kill_sent = false;
        managed.pending_restart = false;
        self.health_states.remove(task_name);
        send_graceful_terminate(&managed.child)
            .await
            .with_context(|| format!("failed to stop task [{task_name}]"))?;
        self.push_task_event(
            task_name,
            EventType::TaskStopped,
            Some(status_before),
            Some(TaskStatus::Stopping),
            Some("stop".to_string()),
        );
        self.describe_task(task_name)
    }

    pub async fn restart_task(&mut self, task_name: &str) -> Result<TaskInfo> {
        if let Some(managed) = self.running.get_mut(task_name) {
            if managed.status == TaskStatus::Restarting {
                return self.describe_task(task_name);
            }
            let status_before = managed.status;
            managed.status = TaskStatus::Restarting;
            managed.stop_requested_at = Some(Instant::now());
            managed.force_kill_sent = false;
            managed.pending_restart = true;
            self.health_states.remove(task_name);
            send_graceful_terminate(&managed.child)
                .await
                .with_context(|| format!("failed to restart task [{task_name}]"))?;
            self.push_task_event(
                task_name,
                EventType::TaskRestarted,
                Some(status_before),
                Some(TaskStatus::Restarting),
                Some("restart".to_string()),
            );
            return self.describe_task(task_name);
        }

        if let Some(pid) = self.restored_pids.get(task_name).copied() {
            if let Some(request) = self.restored_stop_requests.get(task_name) {
                if request.status == TaskStatus::Restarting {
                    return self.describe_task(task_name);
                }
            }
            if !is_process_alive(pid) {
                self.restored_pids.remove(task_name);
                self.restored_stop_requests.remove(task_name);
                self.record_restored_task_stop(task_name, None);
                return self.start_task(task_name).await;
            }
            send_term_to_pid(pid)
                .await
                .with_context(|| format!("failed to restart restored task [{task_name}]"))?;
            self.restored_stop_requests.insert(
                task_name.to_string(),
                RestoredStopRequest {
                    status: TaskStatus::Restarting,
                    requested_at: Instant::now(),
                    force_kill_sent: false,
                    pending_restart: true,
                },
            );
            self.health_states.remove(task_name);
            self.push_task_event(
                task_name,
                EventType::TaskRestarted,
                Some(TaskStatus::Online),
                Some(TaskStatus::Restarting),
                Some("restart".to_string()),
            );
            return self.describe_task(task_name);
        }

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
        let kill_timeout = self.kill_timeout();

        for (task_name, managed) in &mut self.running {
            if matches!(
                managed.status,
                TaskStatus::Stopping | TaskStatus::Restarting
            ) && !managed.force_kill_sent
                && managed
                    .stop_requested_at
                    .is_some_and(|requested_at| requested_at.elapsed() >= kill_timeout)
            {
                managed
                    .child
                    .start_kill()
                    .with_context(|| format!("failed to force kill task [{task_name}]"))?;
                managed.force_kill_sent = true;
            }
            if let Some(status) = managed
                .child
                .try_wait()
                .with_context(|| format!("failed to poll task [{task_name}]"))?
            {
                exited.push((task_name.clone(), status.code()));
            }
        }
        for (task_name, pid) in &self.restored_pids {
            if let Some(request) = self.restored_stop_requests.get_mut(task_name) {
                if !request.force_kill_sent && request.requested_at.elapsed() >= kill_timeout {
                    force_kill_pid(*pid).await.with_context(|| {
                        format!("failed to force kill restored task [{task_name}]")
                    })?;
                    request.force_kill_sent = true;
                }
            }
            if !is_process_alive(*pid) {
                exited.push((task_name.clone(), None));
            }
        }

        let mut restarted = Vec::new();
        for (task_name, exit_code) in exited {
            let mut pending_manual_restart = false;
            let mut status_before = TaskStatus::Online;
            if let Some(managed) = self.running.remove(&task_name) {
                pending_manual_restart = managed.pending_restart;
                status_before = managed.status;
                self.record_task_stop(&task_name, &managed, exit_code);
            } else if self.restored_pids.remove(&task_name).is_some() {
                if let Some(request) = self.restored_stop_requests.remove(&task_name) {
                    pending_manual_restart = request.pending_restart;
                    status_before = request.status;
                }
                self.record_restored_task_stop(&task_name, exit_code);
            }
            self.health_states.remove(&task_name);
            self.push_task_event(
                &task_name,
                EventType::TaskStopped,
                Some(status_before),
                Some(TaskStatus::Stopped),
                Some("exit".to_string()),
            );

            if pending_manual_restart {
                restarted.push(self.start_task(&task_name).await?);
            } else if status_before != TaskStatus::Stopping
                && self.should_restart(&task_name, exit_code)?
            {
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
        for (task_name, pid) in self.running_pids() {
            let Some(limit) = self
                .config
                .task(task_name)?
                .limits
                .as_ref()
                .and_then(|limits| limits.max_memory_bytes)
            else {
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
        let running_tasks = self
            .running
            .keys()
            .chain(self.restored_pids.keys())
            .cloned()
            .collect::<Vec<_>>();

        for task_name in running_tasks {
            if self.running.get(&task_name).is_some_and(|managed| {
                matches!(
                    managed.status,
                    TaskStatus::Stopping | TaskStatus::Restarting
                )
            }) || self.restored_stop_requests.contains_key(&task_name)
            {
                self.health_states.remove(&task_name);
                continue;
            }

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
            if let Some(pid) = self.restored_pids.remove(task_name) {
                while is_process_alive(pid) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                self.record_restored_task_stop(task_name, None);
                let info = self.describe_stopped_task(task_name, None, Utc::now())?;
                self.health_states.remove(task_name);
                self.push_task_event(
                    task_name,
                    EventType::TaskStopped,
                    Some(TaskStatus::Online),
                    Some(TaskStatus::Stopped),
                    Some("exit".to_string()),
                );
                return Ok(info);
            }
            return self.describe_stopped_task(task_name, None, Utc::now());
        };

        let status = managed
            .child
            .wait()
            .await
            .with_context(|| format!("failed to wait for task [{task_name}]"))?;
        let exit_code = status.code();
        let status_before = managed.status;
        self.record_task_stop(task_name, &managed, exit_code);
        let info = self.describe_stopped_task(task_name, exit_code, Utc::now())?;
        self.health_states.remove(task_name);
        self.push_task_event(
            task_name,
            EventType::TaskStopped,
            Some(status_before),
            Some(TaskStatus::Stopped),
            Some("exit".to_string()),
        );
        Ok(info)
    }

    pub fn describe_task(&self, task_name: &str) -> Result<TaskInfo> {
        self.describe_task_at(task_name, Utc::now())
    }

    pub fn describe_task_at(&self, task_name: &str, now: DateTime<Utc>) -> Result<TaskInfo> {
        let task = self
            .config
            .task(task_name)
            .with_context(|| format!("task [{task_name}] not found"))?;
        let dependents = self.dependents(task_name);

        if let Some(managed) = self.running.get(task_name) {
            let uptime_ms = managed.started_at.elapsed().as_millis() as u64;
            let health = self.health_states.get(task_name).copied();
            let status = match managed.status {
                TaskStatus::Stopping | TaskStatus::Restarting => managed.status,
                _ => match health {
                    Some(true) => TaskStatus::Healthy,
                    Some(false) => TaskStatus::Unhealthy,
                    None => managed.status,
                },
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
                cpu_percent: managed.child.id().and_then(process_cpu_percent),
                memory_bytes: managed.child.id().and_then(process_memory_bytes),
                restart_count: self.restart_counts.get(task_name).copied().unwrap_or(0),
                last_exit_code: managed.last_exit_code,
                cwd: task.cwd.clone(),
                cmd: task.cmd.clone(),
                dependencies: task.depends_on.clone(),
                dependents,
                schedule_state: self.next_schedule_state(task_name, now)?,
                display_timezone: Some(self.display_timezone().to_string()),
            });
        }
        if let Some(pid) = self.restored_pids.get(task_name).copied() {
            if is_process_alive(pid) {
                let started_at = self.last_started_at.get(task_name).copied();
                let uptime_ms = started_at.map(|started_at| {
                    now.signed_duration_since(started_at)
                        .num_milliseconds()
                        .max(0) as u64
                });
                let health = self.health_states.get(task_name).copied();
                let pending_status = self
                    .restored_stop_requests
                    .get(task_name)
                    .map(|request| request.status);
                let status = match pending_status {
                    Some(TaskStatus::Stopping | TaskStatus::Restarting) => pending_status.unwrap(),
                    _ => match health {
                        Some(true) => TaskStatus::Healthy,
                        Some(false) => TaskStatus::Unhealthy,
                        None => TaskStatus::Online,
                    },
                };
                return Ok(TaskInfo {
                    task_id: self.task_id(task_name)?,
                    name: task_name.to_string(),
                    run_mode: self.task_run_mode(task),
                    pid: Some(pid),
                    status,
                    health: health.map(|is_healthy| {
                        if is_healthy {
                            "ok".to_string()
                        } else {
                            "fail".to_string()
                        }
                    }),
                    started_at,
                    stopped_at: None,
                    uptime_ms,
                    cpu_percent: process_cpu_percent(pid),
                    memory_bytes: process_memory_bytes(pid),
                    restart_count: self.restart_counts.get(task_name).copied().unwrap_or(0),
                    last_exit_code: self.last_exit_codes.get(task_name).copied().flatten(),
                    cwd: task.cwd.clone(),
                    cmd: task.cmd.clone(),
                    dependencies: task.depends_on.clone(),
                    dependents,
                    schedule_state: self.next_schedule_state(task_name, now)?,
                    display_timezone: Some(self.display_timezone().to_string()),
                });
            }
        }

        self.describe_stopped_task(task_name, None, now)
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

    fn describe_stopped_task(
        &self,
        task_name: &str,
        exit_code: Option<i32>,
        now: DateTime<Utc>,
    ) -> Result<TaskInfo> {
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
            cpu_percent: None,
            memory_bytes: None,
            restart_count: self.restart_counts.get(task_name).copied().unwrap_or(0),
            last_exit_code: exit_code
                .or_else(|| self.last_exit_codes.get(task_name).copied().flatten()),
            cwd: task.cwd.clone(),
            cmd: task.cmd.clone(),
            dependencies: task.depends_on.clone(),
            dependents: self.dependents(task_name),
            schedule_state: self.next_schedule_state(task_name, now)?,
            display_timezone: Some(self.display_timezone().to_string()),
        })
    }

    fn next_schedule_state(&self, task_name: &str, now: DateTime<Utc>) -> Result<Option<String>> {
        Ok(next_scheduled_action(&self.config, task_name, now)?
            .map(|action| format_next_action(action, self.display_timezone())))
    }

    fn display_timezone(&self) -> &str {
        &self.config.project.display_timezone
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

    fn record_restored_task_stop(&mut self, task_name: &str, exit_code: Option<i32>) {
        let stopped_at = Utc::now();
        self.last_stopped_at
            .insert(task_name.to_string(), stopped_at);
        if let Some(started_at) = self.last_started_at.get(task_name) {
            self.last_uptime_ms.insert(
                task_name.to_string(),
                stopped_at
                    .signed_duration_since(*started_at)
                    .num_milliseconds()
                    .max(0) as u64,
            );
        }
        self.last_exit_codes
            .insert(task_name.to_string(), exit_code);
    }

    fn kill_timeout(&self) -> Duration {
        self.config
            .defaults
            .kill_timeout
            .as_deref()
            .and_then(parse_duration)
            .unwrap_or_else(|| Duration::from_secs(5))
    }

    fn restored_pid_if_alive(&self, task_name: &str) -> Option<u32> {
        self.restored_pids
            .get(task_name)
            .copied()
            .filter(|pid| is_process_alive(*pid))
    }

    fn running_pids(&self) -> Vec<(&String, u32)> {
        self.running
            .iter()
            .filter_map(|(task_name, managed)| managed.child.id().map(|pid| (task_name, pid)))
            .chain(
                self.restored_pids.iter().filter_map(|(task_name, pid)| {
                    is_process_alive(*pid).then_some((task_name, *pid))
                }),
            )
            .collect()
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

    fn restore_events_from_log(&mut self) {
        let Some(path) = &self.event_log_path else {
            return;
        };
        let Ok(text) = fs::read_to_string(path) else {
            return;
        };

        for line in text.lines() {
            let Ok(event) = serde_json::from_str::<TaskEvent>(line) else {
                continue;
            };
            self.restore_state_from_event(&event);
            self.events.push(event);
        }
    }

    fn restore_state_from_event(&mut self, event: &TaskEvent) {
        let Some(task_name) = &event.task else {
            return;
        };
        if !self.config.tasks.contains_key(task_name) {
            return;
        }

        match event.event_type {
            EventType::TaskStarted => {
                self.last_started_at
                    .insert(task_name.clone(), event.timestamp);
                self.last_stopped_at.remove(task_name);
                self.last_uptime_ms.remove(task_name);
                self.last_exit_codes.remove(task_name);
                if let Some(pid) = event.pid.filter(|pid| is_process_alive(*pid)) {
                    self.restored_pids.insert(task_name.clone(), pid);
                }
            }
            EventType::TaskStopped | EventType::TaskExited => {
                self.restored_pids.remove(task_name);
                self.last_stopped_at
                    .insert(task_name.clone(), event.timestamp);
                self.last_exit_codes
                    .insert(task_name.clone(), event.exit_code);
                if let Some(started_at) = self.last_started_at.get(task_name) {
                    let uptime_ms = event
                        .timestamp
                        .signed_duration_since(*started_at)
                        .num_milliseconds()
                        .max(0) as u64;
                    self.last_uptime_ms.insert(task_name.clone(), uptime_ms);
                }
            }
            _ => {}
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

#[cfg(unix)]
async fn send_term_to_pid(pid: u32) -> Result<()> {
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() && is_process_alive(pid) {
        anyhow::bail!("failed to send TERM to pid [{pid}]");
    }
    Ok(())
}

#[cfg(windows)]
async fn send_term_to_pid(pid: u32) -> Result<()> {
    let status = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() && is_process_alive(pid) {
        anyhow::bail!("failed to stop pid [{pid}] with taskkill");
    }
    Ok(())
}

#[cfg(unix)]
async fn force_kill_pid(pid: u32) -> Result<()> {
    let status = Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() && is_process_alive(pid) {
        anyhow::bail!("failed to send KILL to pid [{pid}]");
    }
    Ok(())
}

#[cfg(windows)]
async fn force_kill_pid(pid: u32) -> Result<()> {
    let status = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() && is_process_alive(pid) {
        anyhow::bail!("failed to force stop pid [{pid}] with taskkill");
    }
    Ok(())
}

#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    StdCommand::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
fn is_process_alive(pid: u32) -> bool {
    let Ok(output) = StdCommand::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
    else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
}

async fn send_graceful_terminate(child: &tokio::process::Child) -> Result<()> {
    let Some(pid) = child.id() else {
        return Ok(());
    };
    if cfg!(target_os = "windows") {
        return Ok(());
    }
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .with_context(|| format!("failed to send TERM to pid [{pid}]"))?;
    if !status.success() {
        anyhow::bail!("failed to send TERM to pid [{pid}]");
    }
    Ok(())
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

fn format_next_action(action: ScheduledAction, display_timezone: &str) -> String {
    let action_label = match action.kind {
        ScheduledActionKind::Start => "start",
        ScheduledActionKind::Stop => "stop",
        ScheduledActionKind::Restart => "restart",
        ScheduledActionKind::Reload => "reload",
        ScheduledActionKind::Command => "command",
    };
    format!(
        "{} {}",
        action_label,
        format_display_table_time(&action.due_at, Some(display_timezone))
    )
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

#[derive(Debug, Clone, Copy)]
pub struct ProcCpuSample {
    pub process_ticks: u64,
    pub process_start_ticks_since_boot: u64,
    pub uptime_ticks_since_boot: u64,
    pub clock_ticks_per_second: u64,
    pub cpu_count: usize,
}

pub fn cpu_percent_from_proc_samples(sample: ProcCpuSample) -> Option<f64> {
    let elapsed_ticks = sample
        .uptime_ticks_since_boot
        .checked_sub(sample.process_start_ticks_since_boot)?;
    if elapsed_ticks == 0 || sample.clock_ticks_per_second == 0 || sample.cpu_count == 0 {
        return None;
    }
    let process_seconds = sample.process_ticks as f64 / sample.clock_ticks_per_second as f64;
    let elapsed_seconds = elapsed_ticks as f64 / sample.clock_ticks_per_second as f64;
    Some((process_seconds / elapsed_seconds) * 100.0 / sample.cpu_count as f64)
}

#[cfg(target_os = "linux")]
pub fn process_cpu_percent(pid: u32) -> Option<f64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (process_ticks, process_start_ticks_since_boot) = parse_process_stat_cpu(&stat)?;
    let uptime = fs::read_to_string("/proc/uptime").ok()?;
    let uptime_seconds = uptime.split_whitespace().next()?.parse::<f64>().ok()?;
    let clock_ticks_per_second = 100;
    let uptime_ticks_since_boot = (uptime_seconds * clock_ticks_per_second as f64) as u64;
    let cpu_count = std::thread::available_parallelism().ok()?.get();

    cpu_percent_from_proc_samples(ProcCpuSample {
        process_ticks,
        process_start_ticks_since_boot,
        uptime_ticks_since_boot,
        clock_ticks_per_second,
        cpu_count,
    })
}

#[cfg(not(target_os = "linux"))]
pub fn process_cpu_percent(pid: u32) -> Option<f64> {
    let _ = pid;
    None
}

#[cfg(target_os = "linux")]
fn parse_process_stat_cpu(stat: &str) -> Option<(u64, u64)> {
    let end_comm = stat.rfind(") ")?;
    let fields = stat[end_comm + 2..].split_whitespace().collect::<Vec<_>>();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    let start_time = fields.get(19)?.parse::<u64>().ok()?;
    Some((utime + stime, start_time))
}
