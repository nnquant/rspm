use rspm_core::config::{
    ActionKind, BackoffMode, HealthCheck, HealthCheckKind, ProjectConfig, RestartPolicy, StartWhen,
};

const SAMPLE_CONFIG: &str = r#"
[project]
name = "trading-stack"
timezone = "Asia/Shanghai"

[defaults]
restart = "on-failure"
restart_delay = "3s"
max_restarts = 10
backoff = "exponential"
max_backoff = "60s"
kill_timeout = "10s"

[tasks.master]
cmd = "uv"
args = ["run", "ldc-master"]
cwd = "/tmp"
autostart = true
restart = "always"

[tasks.master.env]
RUST_LOG = "info"

[tasks.master.health]
type = "tcp"
address = "127.0.0.1:17690"
interval = "1s"
timeout = "500ms"
success_after = 2
failure_after = 3

[tasks.ctp_md]
cmd = "uv"
args = ["run", "ldc-ctp-md"]
depends_on = ["master"]
start_when = "dependencies_healthy"
restart = "on-failure"

[tasks.ctp_md.schedule]
start = "0 8 * * 1-5"
stop = "0 16 * * 1-5"

[tasks.ctp_md.watch]
paths = ["/tmp/input.txt"]

[tasks.ctp_md.logs]
max_bytes = 1048576
max_age_seconds = 86400

[tasks.strategy]
cmd = "uv"
args = ["run", "python", "scripts/run_strategy.py"]
depends_on = ["ctp_md"]
restart = "on-failure"

[tasks.strategy.cron.daily_restart]
expr = "30 8 * * 1-5"
action = "restart"
"#;

#[test]
fn parses_project_defaults_tasks_health_schedule_and_cron() {
    let config = ProjectConfig::from_toml_str(SAMPLE_CONFIG).expect("valid config");

    assert_eq!(config.project.name, "trading-stack");
    assert_eq!(config.project.timezone, "Asia/Shanghai");
    assert_eq!(config.project.display_timezone, "local");
    assert_eq!(config.defaults.restart, RestartPolicy::OnFailure);
    assert_eq!(config.defaults.restart_delay.as_deref(), Some("3s"));
    assert_eq!(config.defaults.backoff, BackoffMode::Exponential);
    assert_eq!(config.defaults.max_backoff.as_deref(), Some("60s"));

    let master = config.task("master").expect("master task");
    assert_eq!(master.cmd, "uv");
    assert_eq!(master.args, vec!["run", "ldc-master"]);
    assert_eq!(master.cwd.as_deref(), Some("/tmp"));
    assert!(master.autostart);
    assert_eq!(master.restart, Some(RestartPolicy::Always));
    assert_eq!(master.env.get("RUST_LOG").map(String::as_str), Some("info"));
    assert_eq!(
        master.health,
        Some(HealthCheck {
            kind: HealthCheckKind::Tcp,
            address: Some("127.0.0.1:17690".to_string()),
            url: None,
            command: None,
            path: None,
            interval: Some("1s".to_string()),
            timeout: Some("500ms".to_string()),
            success_after: Some(2),
            failure_after: Some(3),
        })
    );

    let ctp_md = config.task("ctp_md").expect("ctp_md task");
    assert_eq!(ctp_md.depends_on, vec!["master"]);
    assert_eq!(ctp_md.start_when, StartWhen::DependenciesHealthy);
    assert_eq!(
        ctp_md.schedule.as_ref().unwrap().start.as_deref(),
        Some("0 8 * * 1-5")
    );
    assert_eq!(
        ctp_md.schedule.as_ref().unwrap().stop.as_deref(),
        Some("0 16 * * 1-5")
    );
    assert_eq!(ctp_md.watch.as_ref().unwrap().paths, vec!["/tmp/input.txt"]);
    assert_eq!(ctp_md.logs.as_ref().unwrap().max_bytes, Some(1048576));
    assert_eq!(ctp_md.logs.as_ref().unwrap().max_age_seconds, Some(86400));

    let strategy = config.task("strategy").expect("strategy task");
    let restart = strategy
        .cron
        .get("daily_restart")
        .expect("daily restart cron");
    assert_eq!(restart.expr, "30 8 * * 1-5");
    assert_eq!(restart.action, ActionKind::Restart);
}

#[test]
fn applies_documented_defaults_when_task_fields_are_absent() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "minimal"

        [tasks.worker]
        cmd = "python"
        "#,
    )
    .expect("valid config");

    assert_eq!(config.project.timezone, "UTC");
    assert_eq!(config.project.display_timezone, "local");
    assert_eq!(config.defaults.restart, RestartPolicy::Never);
    assert_eq!(config.defaults.backoff, BackoffMode::None);

    let worker = config.task("worker").expect("worker task");
    assert!(!worker.autostart);
    assert_eq!(worker.start_when, StartWhen::DependenciesHealthy);
    assert!(worker.args.is_empty());
    assert!(worker.depends_on.is_empty());
}

#[test]
fn parses_project_display_timezone_override() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "display-timezone"
        display_timezone = "Asia/Shanghai"

        [tasks.worker]
        cmd = "true"
        "#,
    )
    .expect("valid config");

    assert_eq!(config.project.display_timezone, "Asia/Shanghai");
}

#[test]
fn rejects_invalid_project_display_timezone() {
    let error = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "bad-display-timezone"
        display_timezone = "Mars/Base"

        [tasks.worker]
        cmd = "true"
        "#,
    )
    .expect_err("invalid display timezone");

    assert!(error.to_string().contains("display_timezone"));
}

#[test]
fn rejects_executable_configuration_that_is_not_toml() {
    let error = ProjectConfig::from_toml_str("module.exports = {}").expect_err("invalid toml");
    assert!(error.to_string().contains("at least one"));
}
