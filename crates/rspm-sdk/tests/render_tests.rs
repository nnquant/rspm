use chrono::{TimeZone, Utc};
use rspm_core::state::{TaskInfo, TaskStatus};
use rspm_sdk::render::{
    format_merged_logs, format_prefixed_logs, format_task_table, RenderLogOptions,
};

fn strip_ansi(value: &str) -> String {
    let mut output = String::new();
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for code_ch in chars.by_ref() {
                if code_ch == 'm' {
                    break;
                }
            }
            continue;
        }
        output.push(ch);
    }
    output
}

fn task(name: &str, status: TaskStatus) -> TaskInfo {
    TaskInfo {
        task_id: 1,
        name: name.to_string(),
        run_mode: "long".to_string(),
        pid: Some(42),
        status,
        health: Some("ok".to_string()),
        started_at: Some(Utc.with_ymd_and_hms(2026, 5, 20, 1, 2, 3).unwrap()),
        stopped_at: None,
        uptime_ms: Some(61_000),
        cpu_percent: Some(80.0),
        memory_bytes: Some(512 * 1024 * 1024),
        restart_count: 3,
        last_exit_code: None,
        cwd: None,
        cmd: "uv".to_string(),
        dependencies: Vec::new(),
        dependents: Vec::new(),
        schedule_state: Some("start 05-20 09:30:00".to_string()),
        display_timezone: Some("Asia/Shanghai".to_string()),
    }
}

#[test]
fn render_task_table_matches_cli_style() {
    let output = format_task_table(&[task("market", TaskStatus::Online)]);

    assert!(output.contains("TASK_ID"));
    assert!(output.contains("START_TIME"));
    assert!(output.contains("market"));
    assert!(output.contains("05-20 09:02:03"));
    assert!(output.contains("\x1b[32monline"));
    assert!(output.contains("\x1b[33m3       \x1b[0m"));
    assert!(output.contains("\x1b[90mTimezone: Asia/Shanghai\x1b[0m"));
}

#[test]
fn render_task_table_keeps_columns_aligned_for_long_task_name() {
    let output = strip_ansi(&format_task_table(&[task(
        "ldc-ctp-bond-future-factors",
        TaskStatus::Online,
    )]));
    let mut lines = output.lines();
    let header = lines.next().unwrap();
    let row = lines.next().unwrap();

    assert_eq!(header.find("MODE"), row.find("long"));
    assert!(row.contains("ldc-ctp-bond-future-factors"));
    assert!(!row.contains("..."));
}

#[test]
fn render_prefixed_logs_keeps_terminal_styles() {
    let output = format_prefixed_logs(
        "market",
        "\x1b[32mINFO\x1b[0m started\n",
        &RenderLogOptions::default(),
    );

    assert_eq!(output, "market | \x1b[32mINFO\x1b[0m started\n");
}

#[test]
fn render_merged_logs_orders_timestamped_lines() {
    let logs = vec![
        (
            "beta".to_string(),
            "2026-05-20T01:00:02Z beta\n".to_string(),
        ),
        (
            "alpha".to_string(),
            "2026-05-20T01:00:01Z alpha\n".to_string(),
        ),
    ];
    let output = format_merged_logs(&logs, &RenderLogOptions::default());

    assert_eq!(
        output,
        "alpha | 2026-05-20T01:00:01Z alpha\nbeta | 2026-05-20T01:00:02Z beta\n"
    );
}
