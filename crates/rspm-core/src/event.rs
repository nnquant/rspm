use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::state::TaskStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    TaskStarted,
    TaskHealthy,
    TaskUnhealthy,
    TaskExited,
    TaskRestarted,
    TaskStopped,
    DependencyWaiting,
    ScheduleTriggered,
    CronTriggered,
    ConfigApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEvent {
    pub timestamp: DateTime<Utc>,
    pub project: String,
    pub task: Option<String>,
    pub event_type: EventType,
    pub status_before: Option<TaskStatus>,
    pub status_after: Option<TaskStatus>,
    pub reason: Option<String>,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub message: Option<String>,
}

impl TaskEvent {
    pub fn new(project: impl Into<String>, event_type: EventType) -> Self {
        Self {
            timestamp: Utc::now(),
            project: project.into(),
            task: None,
            event_type,
            status_before: None,
            status_after: None,
            reason: None,
            pid: None,
            exit_code: None,
            signal: None,
            message: None,
        }
    }
}
