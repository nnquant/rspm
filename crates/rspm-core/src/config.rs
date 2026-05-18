use std::collections::BTreeMap;
use std::str::FromStr;

use cron::Schedule;
use serde::{Deserialize, Serialize};

use crate::error::ConfigError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    #[serde(default)]
    pub project: ProjectSection,
    #[serde(default)]
    pub defaults: DefaultsSection,
    #[serde(default)]
    pub tasks: BTreeMap<String, TaskConfig>,
}

impl ProjectConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(input)?;
        config.validate()?;
        Ok(config)
    }

    pub fn task(&self, name: &str) -> Result<&TaskConfig, ConfigError> {
        self.tasks
            .get(name)
            .ok_or_else(|| ConfigError::TaskNotFound(name.to_string()))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.tasks.is_empty() {
            return Err(ConfigError::Validation(
                "at least one [tasks.<name>] section is required".to_string(),
            ));
        }

        for (task_name, task) in &self.tasks {
            if let Some(schedule) = &task.schedule {
                if let Some(expr) = &schedule.start {
                    validate_cron_expr(expr, task_name, "schedule.start")?;
                }
                if let Some(expr) = &schedule.stop {
                    validate_cron_expr(expr, task_name, "schedule.stop")?;
                }
            }

            for (cron_name, cron_action) in &task.cron {
                validate_cron_expr(&cron_action.expr, task_name, &format!("cron.{cron_name}"))?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectSection {
    pub name: String,
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

impl Default for ProjectSection {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            timezone: default_timezone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultsSection {
    #[serde(default)]
    pub restart: RestartPolicy,
    pub restart_delay: Option<String>,
    pub max_restarts: Option<u32>,
    #[serde(default)]
    pub backoff: BackoffMode,
    pub max_backoff: Option<String>,
    pub kill_timeout: Option<String>,
}

impl Default for DefaultsSection {
    fn default() -> Self {
        Self {
            restart: RestartPolicy::Never,
            restart_delay: None,
            max_restarts: None,
            backoff: BackoffMode::None,
            max_backoff: None,
            kill_timeout: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskConfig {
    pub cmd: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub autostart: bool,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub restart: Option<RestartPolicy>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub start_when: StartWhen,
    pub health: Option<HealthCheck>,
    pub schedule: Option<ScheduleConfig>,
    pub watch: Option<WatchConfig>,
    pub limits: Option<LimitConfig>,
    pub logs: Option<LogConfig>,
    pub reload: Option<ReloadConfig>,
    #[serde(default)]
    pub cron: BTreeMap<String, CronAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheck {
    #[serde(rename = "type")]
    pub kind: HealthCheckKind,
    pub address: Option<String>,
    pub url: Option<String>,
    pub command: Option<String>,
    pub path: Option<String>,
    pub interval: Option<String>,
    pub timeout: Option<String>,
    pub success_after: Option<u32>,
    pub failure_after: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HealthCheckKind {
    Tcp,
    Http,
    Command,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleConfig {
    pub start: Option<String>,
    pub stop: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchConfig {
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitConfig {
    pub max_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogConfig {
    pub max_bytes: Option<u64>,
    pub max_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReloadConfig {
    pub mode: ReloadMode,
    pub command: Option<String>,
    pub signal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReloadMode {
    Command,
    Signal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronAction {
    pub expr: String,
    pub action: ActionKind,
    pub command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ActionKind {
    Start,
    Stop,
    Restart,
    Reload,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestartPolicy {
    Never,
    #[serde(rename = "on-failure")]
    OnFailure,
    Always,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::Never
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackoffMode {
    None,
    Exponential,
}

impl Default for BackoffMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartWhen {
    DependenciesStarted,
    DependenciesHealthy,
    Manual,
}

impl Default for StartWhen {
    fn default() -> Self {
        Self::DependenciesHealthy
    }
}

fn default_timezone() -> String {
    "UTC".to_string()
}

pub fn normalize_cron_expr(expr: &str) -> String {
    let fields = expr.split_whitespace().collect::<Vec<_>>();
    if fields.len() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    }
}

fn validate_cron_expr(expr: &str, task_name: &str, field: &str) -> Result<(), ConfigError> {
    let expr = normalize_cron_expr(expr);
    Schedule::from_str(&expr).map_err(|source| {
        let label = if field.starts_with("cron.") {
            "invalid cron"
        } else {
            "invalid schedule"
        };
        ConfigError::Validation(format!(
            "{label} for task [{task_name}] field [{field}]: {source}"
        ))
    })?;
    Ok(())
}
