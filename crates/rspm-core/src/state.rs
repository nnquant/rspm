use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Defined,
    Scheduled,
    WaitingDependency,
    Starting,
    Online,
    Healthy,
    Unhealthy,
    Stopping,
    Restarting,
    Stopped,
    Failed,
    Backoff,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskInfo {
    #[serde(default)]
    pub task_id: u32,
    pub name: String,
    #[serde(default)]
    pub run_mode: String,
    pub pid: Option<u32>,
    pub status: TaskStatus,
    pub health: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub uptime_ms: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub memory_bytes: Option<u64>,
    pub restart_count: u32,
    pub last_exit_code: Option<i32>,
    pub cwd: Option<String>,
    pub cmd: String,
    pub dependencies: Vec<String>,
    pub dependents: Vec<String>,
    pub schedule_state: Option<String>,
    #[serde(default)]
    pub display_timezone: Option<String>,
}
