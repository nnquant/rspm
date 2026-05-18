use anyhow::Result;
use chrono::{DateTime, Utc};
use rspm_core::event::EventType;
use rspm_core::schedule::{collect_due_actions, ScheduledAction, ScheduledActionKind};

use crate::orchestrator::start_task_tree;
use crate::runtime::TaskRuntime;

pub async fn run_due_actions(
    runtime: &mut TaskRuntime,
    last_tick: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<ScheduledAction>> {
    let actions = collect_due_actions(runtime.config(), last_tick, now)?;

    for action in &actions {
        runtime.record_task_event(
            &action.task,
            if action.name.is_some() {
                EventType::CronTriggered
            } else {
                EventType::ScheduleTriggered
            },
            action.name.as_deref().unwrap_or("schedule"),
        );
        match action.kind {
            ScheduledActionKind::Start => {
                let _ = start_task_tree(runtime, &action.task).await?;
            }
            ScheduledActionKind::Stop => {
                let _ = runtime.stop_task(&action.task).await?;
            }
            ScheduledActionKind::Restart => {
                let _ = runtime.restart_task(&action.task).await?;
            }
            ScheduledActionKind::Reload => {
                let _ = runtime.reload_task(&action.task).await?;
            }
            ScheduledActionKind::Command => {
                run_one_shot_command(action.command.as_deref()).await?;
            }
        }
    }

    Ok(actions)
}

async fn run_one_shot_command(command: Option<&str>) -> Result<()> {
    let Some(command) = command else {
        return Ok(());
    };
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
        anyhow::bail!("scheduled command failed");
    }
    Ok(())
}
