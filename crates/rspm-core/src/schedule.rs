use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use cron::Schedule;

use crate::config::{normalize_cron_expr, ActionKind, CronAction, ProjectConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledAction {
    pub task: String,
    pub kind: ScheduledActionKind,
    pub name: Option<String>,
    pub command: Option<String>,
    pub due_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledActionKind {
    Start,
    Stop,
    Restart,
    Reload,
    Command,
}

#[derive(Debug, Clone)]
pub struct ParsedCronAction {
    pub name: String,
    pub action: ActionKind,
    pub command: Option<String>,
    schedule: Schedule,
}

struct ExprActionSpec<'a> {
    task_name: &'a str,
    name: Option<&'a String>,
    expr: &'a str,
    kind: ScheduledActionKind,
    command: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct DueWindow {
    last_tick: DateTime<Utc>,
    now: DateTime<Utc>,
    timezone: ProjectTimezone,
}

#[derive(Debug, Clone, Copy)]
enum ProjectTimezone {
    Iana(Tz),
    Offset(Duration),
}

pub fn collect_due_actions(
    config: &ProjectConfig,
    last_tick: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<ScheduledAction>, cron::error::Error> {
    let mut actions = Vec::new();
    let window = DueWindow {
        last_tick,
        now,
        timezone: project_timezone(&config.project.timezone),
    };

    for (task_name, task) in &config.tasks {
        if let Some(schedule) = &task.schedule {
            if let Some(expr) = &schedule.start {
                collect_expr_actions(
                    &mut actions,
                    ExprActionSpec {
                        task_name,
                        name: None,
                        expr,
                        kind: ScheduledActionKind::Start,
                        command: None,
                    },
                    window,
                )?;
            }
            if let Some(expr) = &schedule.stop {
                collect_expr_actions(
                    &mut actions,
                    ExprActionSpec {
                        task_name,
                        name: None,
                        expr,
                        kind: ScheduledActionKind::Stop,
                        command: None,
                    },
                    window,
                )?;
            }
        }

        for (name, cron_action) in &task.cron {
            collect_expr_actions(
                &mut actions,
                ExprActionSpec {
                    task_name,
                    name: Some(name),
                    expr: &cron_action.expr,
                    kind: ScheduledActionKind::from_action_kind(&cron_action.action),
                    command: cron_action.command.clone(),
                },
                window,
            )?;
        }
    }

    actions.sort_by(|left, right| {
        left.due_at
            .cmp(&right.due_at)
            .then_with(|| left.task.cmp(&right.task))
    });
    Ok(actions)
}

pub fn next_scheduled_action(
    config: &ProjectConfig,
    task_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<ScheduledAction>, cron::error::Error> {
    let Some(task) = config.tasks.get(task_name) else {
        return Ok(None);
    };
    let timezone = project_timezone(&config.project.timezone);
    let mut actions = Vec::new();

    if let Some(schedule) = &task.schedule {
        if let Some(expr) = &schedule.start {
            if let Some(due_at) = next_expr_due_at(expr, now, timezone)? {
                actions.push(ScheduledAction {
                    task: task_name.to_string(),
                    kind: ScheduledActionKind::Start,
                    name: None,
                    command: None,
                    due_at,
                });
            }
        }
        if let Some(expr) = &schedule.stop {
            if let Some(due_at) = next_expr_due_at(expr, now, timezone)? {
                actions.push(ScheduledAction {
                    task: task_name.to_string(),
                    kind: ScheduledActionKind::Stop,
                    name: None,
                    command: None,
                    due_at,
                });
            }
        }
    }

    for (name, cron_action) in &task.cron {
        if let Some(due_at) = next_expr_due_at(&cron_action.expr, now, timezone)? {
            actions.push(ScheduledAction {
                task: task_name.to_string(),
                kind: ScheduledActionKind::from_action_kind(&cron_action.action),
                name: Some(name.clone()),
                command: cron_action.command.clone(),
                due_at,
            });
        }
    }

    actions.sort_by(|left, right| {
        left.due_at
            .cmp(&right.due_at)
            .then_with(|| left.task.cmp(&right.task))
    });
    Ok(actions.into_iter().next())
}

pub fn is_task_in_schedule_window(
    config: &ProjectConfig,
    task_name: &str,
    now: DateTime<Utc>,
) -> Result<bool, cron::error::Error> {
    let Some(task) = config.tasks.get(task_name) else {
        return Ok(false);
    };
    let Some(schedule) = &task.schedule else {
        return Ok(false);
    };
    if schedule.start.is_none() || schedule.stop.is_none() {
        return Ok(false);
    }

    let last_tick = now - Duration::days(370);
    let mut latest_window_action = None;
    for action in collect_due_actions(config, last_tick, now)? {
        if action.task != task_name || action.name.is_some() {
            continue;
        }
        if matches!(
            action.kind,
            ScheduledActionKind::Start | ScheduledActionKind::Stop
        ) {
            latest_window_action = Some(action.kind);
        }
    }

    Ok(latest_window_action == Some(ScheduledActionKind::Start))
}

impl ScheduledActionKind {
    fn from_action_kind(action: &ActionKind) -> Self {
        match action {
            ActionKind::Start => Self::Start,
            ActionKind::Stop => Self::Stop,
            ActionKind::Restart => Self::Restart,
            ActionKind::Reload => Self::Reload,
            ActionKind::Command => Self::Command,
        }
    }
}

fn collect_expr_actions(
    actions: &mut Vec<ScheduledAction>,
    spec: ExprActionSpec<'_>,
    window: DueWindow,
) -> Result<(), cron::error::Error> {
    let schedule = Schedule::from_str(&normalize_cron_expr(spec.expr))?;
    match window.timezone {
        ProjectTimezone::Iana(timezone) => {
            let local_last_tick = window.last_tick.with_timezone(&timezone);
            let local_now = window.now.with_timezone(&timezone);
            for due_at in schedule
                .after(&local_last_tick)
                .take_while(|due_at| *due_at <= local_now)
            {
                actions.push(ScheduledAction {
                    task: spec.task_name.to_string(),
                    kind: spec.kind,
                    name: spec.name.cloned(),
                    command: spec.command.clone(),
                    due_at: due_at.with_timezone(&Utc),
                });
            }
        }
        ProjectTimezone::Offset(offset) => {
            let local_last_tick = window.last_tick + offset;
            let local_now = window.now + offset;
            for due_at in schedule
                .after(&local_last_tick)
                .take_while(|due_at| *due_at <= local_now)
            {
                actions.push(ScheduledAction {
                    task: spec.task_name.to_string(),
                    kind: spec.kind,
                    name: spec.name.cloned(),
                    command: spec.command.clone(),
                    due_at: due_at - offset,
                });
            }
        }
    }
    Ok(())
}

fn next_expr_due_at(
    expr: &str,
    now: DateTime<Utc>,
    timezone: ProjectTimezone,
) -> Result<Option<DateTime<Utc>>, cron::error::Error> {
    let schedule = Schedule::from_str(&normalize_cron_expr(expr))?;
    let next = match timezone {
        ProjectTimezone::Iana(timezone) => schedule
            .after(&now.with_timezone(&timezone))
            .next()
            .map(|due_at| due_at.with_timezone(&Utc)),
        ProjectTimezone::Offset(offset) => schedule
            .after(&(now + offset))
            .next()
            .map(|due_at| due_at - offset),
    };
    Ok(next)
}

fn project_timezone(timezone: &str) -> ProjectTimezone {
    timezone
        .parse::<Tz>()
        .map(ProjectTimezone::Iana)
        .or_else(|_| {
            parse_utc_offset(timezone)
                .map(ProjectTimezone::Offset)
                .ok_or(())
        })
        .unwrap_or(ProjectTimezone::Offset(Duration::zero()))
}

fn parse_utc_offset(value: &str) -> Option<Duration> {
    let value = value
        .strip_prefix("UTC")
        .or_else(|| value.strip_prefix("GMT"))?;
    if value.is_empty() {
        return Some(Duration::zero());
    }
    let sign = match &value[..1] {
        "+" => 1,
        "-" => -1,
        _ => return None,
    };
    let rest = &value[1..];
    let (hours, minutes) = if let Some((hours, minutes)) = rest.split_once(':') {
        (hours.parse::<i64>().ok()?, minutes.parse::<i64>().ok()?)
    } else {
        (rest.parse::<i64>().ok()?, 0)
    };
    Some(Duration::minutes(sign * (hours * 60 + minutes)))
}

impl ParsedCronAction {
    pub fn parse(name: impl Into<String>, action: &CronAction) -> Result<Self, cron::error::Error> {
        Ok(Self {
            name: name.into(),
            action: action.action.clone(),
            command: action.command.clone(),
            schedule: Schedule::from_str(&normalize_cron_expr(&action.expr))?,
        })
    }

    pub fn next_after(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule.after(&now).next()
    }
}
