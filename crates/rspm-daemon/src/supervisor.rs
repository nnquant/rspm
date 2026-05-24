use std::time::Instant;

use chrono::{DateTime, Utc};
use rspm_core::state::TaskStatus;
use tokio::process::Child;

pub struct ManagedTask {
    pub child: Child,
    pub started_at: Instant,
    pub started_at_utc: DateTime<Utc>,
    pub restart_count: u32,
    pub last_exit_code: Option<i32>,
    pub status: TaskStatus,
    pub stop_requested_at: Option<Instant>,
    pub force_kill_sent: bool,
    pub pending_restart: bool,
}

impl ManagedTask {
    pub fn new(child: Child) -> Self {
        Self {
            child,
            started_at: Instant::now(),
            started_at_utc: Utc::now(),
            restart_count: 0,
            last_exit_code: None,
            status: TaskStatus::Online,
            stop_requested_at: None,
            force_kill_sent: false,
            pending_restart: false,
        }
    }
}
