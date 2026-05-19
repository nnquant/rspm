use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use rspm_core::dag::TaskGraph;
use rspm_core::schedule::is_task_in_schedule_window;
use std::collections::BTreeSet;
use std::time::Duration;

use rspm_core::config::{HealthCheck, StartWhen};
use rspm_core::state::TaskInfo;

use crate::health::check_health;
use crate::runtime::TaskRuntime;

pub async fn start_all(runtime: &mut TaskRuntime) -> Result<Vec<TaskInfo>> {
    start_planned(runtime, None).await
}

pub async fn start_autostart(runtime: &mut TaskRuntime) -> Result<Vec<TaskInfo>> {
    let mut selected = BTreeSet::new();
    for (task_name, task) in &runtime_config(runtime).tasks {
        if task.autostart {
            collect_with_dependencies(runtime_config(runtime), task_name, &mut selected)?;
        }
    }
    start_planned(runtime, Some(selected)).await
}

pub async fn start_scheduled_active(
    runtime: &mut TaskRuntime,
    now: DateTime<Utc>,
) -> Result<Vec<TaskInfo>> {
    let mut selected = BTreeSet::new();
    for task_name in runtime_config(runtime).tasks.keys() {
        if is_task_in_schedule_window(runtime_config(runtime), task_name, now)? {
            collect_with_dependencies(runtime_config(runtime), task_name, &mut selected)?;
        }
    }
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    start_planned(runtime, Some(selected)).await
}

pub async fn start_task_tree(runtime: &mut TaskRuntime, task_name: &str) -> Result<Vec<TaskInfo>> {
    let mut selected = BTreeSet::new();
    collect_with_dependencies(runtime_config(runtime), task_name, &mut selected)?;
    start_planned(runtime, Some(selected)).await
}

async fn start_planned(
    runtime: &mut TaskRuntime,
    selected: Option<BTreeSet<String>>,
) -> Result<Vec<TaskInfo>> {
    let graph = TaskGraph::from_config(runtime_config(runtime))?;
    let plan = graph.plan_all()?;
    let mut started = Vec::new();
    for task_name in plan.start_order {
        let task = runtime_config(runtime).task(&task_name)?.clone();
        if selected
            .as_ref()
            .is_some_and(|selected| !selected.contains(&task_name))
        {
            continue;
        }
        if task.start_when == StartWhen::Manual {
            continue;
        }
        if task.start_when == StartWhen::DependenciesHealthy {
            ensure_dependencies_healthy(runtime, &task_name).await?;
        }
        let mut info = runtime.start_task(&task_name).await?;
        if let Some(health) = task.health.as_ref() {
            if !wait_for_startup_health(health).await? {
                let _ = runtime.set_task_health(&task_name, false)?;
                bail!("task [{task_name}] failed health check");
            }
            info = runtime.set_task_health(&task_name, true)?;
        }
        started.push(info);
    }
    Ok(started)
}

async fn wait_for_startup_health(health: &HealthCheck) -> Result<bool> {
    let interval = health
        .interval
        .as_deref()
        .and_then(parse_duration)
        .unwrap_or_else(|| Duration::from_millis(100));
    let success_after = health.success_after.unwrap_or(1).max(1);
    let failure_after = health.failure_after.unwrap_or(1).max(1);
    let mut successes = 0_u32;
    let mut failures = 0_u32;
    let mut first_probe = true;

    loop {
        if check_health(health).await? {
            successes += 1;
            failures = 0;
            if successes >= success_after {
                return Ok(true);
            }
        } else {
            successes = 0;
            if !first_probe {
                failures += 1;
            }
            if failures >= failure_after {
                return Ok(false);
            }
        }
        first_probe = false;
        tokio::time::sleep(interval).await;
    }
}

pub async fn stop_all(runtime: &mut TaskRuntime) -> Result<Vec<TaskInfo>> {
    let graph = TaskGraph::from_config(runtime_config(runtime))?;
    let plan = graph.plan_all()?;
    let mut stopped = Vec::new();
    for task_name in plan.stop_order {
        stopped.push(runtime.stop_task(&task_name).await?);
    }
    Ok(stopped)
}

fn runtime_config(runtime: &TaskRuntime) -> &rspm_core::config::ProjectConfig {
    runtime.config()
}

fn collect_with_dependencies(
    config: &rspm_core::config::ProjectConfig,
    task_name: &str,
    selected: &mut BTreeSet<String>,
) -> Result<()> {
    if !selected.insert(task_name.to_string()) {
        return Ok(());
    }
    for dependency in &config.task(task_name)?.depends_on {
        collect_with_dependencies(config, dependency, selected)?;
    }
    Ok(())
}

async fn ensure_dependencies_healthy(runtime: &TaskRuntime, task_name: &str) -> Result<()> {
    let task = runtime_config(runtime).task(task_name)?;
    for dependency in &task.depends_on {
        let dependency_task = runtime_config(runtime).task(dependency)?;
        let dependency_info = runtime.describe_task(dependency)?;
        if dependency_info.pid.is_none() {
            bail!("task [{task_name}] is waiting for dependency [{dependency}]");
        }
        if let Some(health) = &dependency_task.health {
            if !check_health(health).await? {
                bail!("task [{task_name}] dependency [{dependency}] is unhealthy");
            }
        }
    }
    Ok(())
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
