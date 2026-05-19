use chrono::{TimeZone, Utc};
use rspm_core::config::{ActionKind, CronAction, ProjectConfig};
use rspm_core::schedule::{
    collect_due_actions, next_scheduled_action, ParsedCronAction, ScheduledActionKind,
};

#[test]
fn parses_cron_action_and_computes_next_trigger() {
    let action = CronAction {
        expr: "0 30 8 * * *".to_string(),
        action: ActionKind::Restart,
        command: None,
    };

    let parsed = ParsedCronAction::parse("daily_restart", &action).expect("valid cron");
    let now = Utc.with_ymd_and_hms(2026, 5, 17, 8, 0, 0).unwrap();
    let next = parsed.next_after(now).expect("next trigger");

    assert_eq!(parsed.name, "daily_restart");
    assert_eq!(parsed.action, ActionKind::Restart);
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 5, 17, 8, 30, 0).unwrap());
}

#[test]
fn rejects_invalid_schedule_cron_during_config_validation() {
    let error = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "invalid-cron"

        [tasks.worker]
        cmd = "true"

        [tasks.worker.cron.bad]
        expr = "not a cron"
        action = "restart"
        "#,
    )
    .expect_err("invalid cron must be rejected");

    assert!(error.to_string().contains("invalid cron"));
    assert!(error.to_string().contains("worker"));
    assert!(error.to_string().contains("bad"));
}

#[test]
fn rejects_invalid_start_stop_schedule_during_config_validation() {
    let error = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "invalid-schedule"

        [tasks.worker]
        cmd = "true"

        [tasks.worker.schedule]
        start = "bad schedule"
        "#,
    )
    .expect_err("invalid schedule must be rejected");

    assert!(error.to_string().contains("invalid schedule"));
    assert!(error.to_string().contains("worker"));
    assert!(error.to_string().contains("start"));
}

#[test]
fn collects_due_schedule_and_cron_actions_between_ticks() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "schedule-test"

        [tasks.market]
        cmd = "true"

        [tasks.market.schedule]
        start = "30 8 * * *"
        stop = "0 16 * * *"

        [tasks.market.cron.refresh]
        expr = "45 8 * * *"
        action = "restart"
        "#,
    )
    .expect("valid config");
    let last = Utc.with_ymd_and_hms(2026, 5, 18, 8, 29, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 5, 18, 8, 46, 0).unwrap();

    let actions = collect_due_actions(&config, last, now).expect("due actions");

    assert_eq!(actions.len(), 2);
    assert_eq!(actions[0].task, "market");
    assert_eq!(actions[0].kind, ScheduledActionKind::Start);
    assert_eq!(actions[1].task, "market");
    assert_eq!(actions[1].kind, ScheduledActionKind::Restart);
}

#[test]
fn computes_next_scheduled_action_for_task_in_project_timezone() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "next-schedule-test"
        timezone = "Asia/Shanghai"

        [tasks.market]
        cmd = "true"

        [tasks.market.schedule]
        start = "30 8 * * *"
        stop = "0 16 * * *"

        [tasks.market.cron.refresh]
        expr = "45 8 * * *"
        action = "restart"
        "#,
    )
    .expect("valid config");
    let now = Utc.with_ymd_and_hms(2026, 5, 18, 0, 0, 0).unwrap();

    let next = next_scheduled_action(&config, "market", now)
        .expect("parse schedule")
        .expect("next action");

    assert_eq!(next.task, "market");
    assert_eq!(next.kind, ScheduledActionKind::Start);
    assert_eq!(
        next.due_at,
        Utc.with_ymd_and_hms(2026, 5, 18, 0, 30, 0).unwrap()
    );
}

#[test]
fn detects_task_inside_scheduled_start_stop_window() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "schedule-window-test"
        timezone = "Asia/Shanghai"

        [tasks.market]
        cmd = "true"

        [tasks.market.schedule]
        start = "30 8 * * *"
        stop = "00 15 * * *"
        "#,
    )
    .expect("valid config");

    assert!(rspm_core::schedule::is_task_in_schedule_window(
        &config,
        "market",
        Utc.with_ymd_and_hms(2026, 5, 18, 1, 30, 0).unwrap(),
    )
    .expect("window state"));
    assert!(!rspm_core::schedule::is_task_in_schedule_window(
        &config,
        "market",
        Utc.with_ymd_and_hms(2026, 5, 18, 8, 0, 0).unwrap(),
    )
    .expect("window state"));
}

#[test]
fn detects_task_inside_overnight_scheduled_window() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "overnight-window-test"
        timezone = "Asia/Shanghai"

        [tasks.night_market]
        cmd = "true"

        [tasks.night_market.schedule]
        start = "00 21 * * *"
        stop = "30 2 * * *"
        "#,
    )
    .expect("valid config");

    assert!(rspm_core::schedule::is_task_in_schedule_window(
        &config,
        "night_market",
        Utc.with_ymd_and_hms(2026, 5, 18, 14, 0, 0).unwrap(),
    )
    .expect("window state"));
    assert!(!rspm_core::schedule::is_task_in_schedule_window(
        &config,
        "night_market",
        Utc.with_ymd_and_hms(2026, 5, 18, 3, 0, 0).unwrap(),
    )
    .expect("window state"));
}

#[test]
fn interprets_cron_expressions_in_project_timezone() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "timezone-test"
        timezone = "Asia/Shanghai"

        [tasks.market]
        cmd = "true"

        [tasks.market.schedule]
        start = "30 8 * * *"
        "#,
    )
    .expect("valid config");
    let last = Utc.with_ymd_and_hms(2026, 5, 18, 0, 29, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 5, 18, 0, 31, 0).unwrap();

    let actions = collect_due_actions(&config, last, now).expect("due actions");

    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].due_at,
        Utc.with_ymd_and_hms(2026, 5, 18, 0, 30, 0).unwrap()
    );
}

#[test]
fn interprets_cron_expressions_with_iana_timezone_and_dst() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "iana-timezone-test"
        timezone = "America/New_York"

        [tasks.market]
        cmd = "true"

        [tasks.market.schedule]
        start = "30 8 * * *"
        "#,
    )
    .expect("valid config");
    let last = Utc.with_ymd_and_hms(2026, 7, 1, 12, 29, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 31, 0).unwrap();

    let actions = collect_due_actions(&config, last, now).expect("due actions");

    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].due_at,
        Utc.with_ymd_and_hms(2026, 7, 1, 12, 30, 0).unwrap()
    );
}
