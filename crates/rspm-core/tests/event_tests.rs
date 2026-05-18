use chrono::{TimeZone, Utc};
use rspm_core::event::{EventType, TaskEvent};
use rspm_core::state::TaskStatus;

#[test]
fn serializes_task_event_as_audit_friendly_json() {
    let event = TaskEvent {
        timestamp: Utc.with_ymd_and_hms(2026, 5, 17, 8, 30, 0).unwrap(),
        project: "trading-stack".to_string(),
        task: Some("ctp_md".to_string()),
        event_type: EventType::TaskRestarted,
        status_before: Some(TaskStatus::Healthy),
        status_after: Some(TaskStatus::Starting),
        reason: Some("cron".to_string()),
        pid: Some(1234),
        exit_code: None,
        signal: None,
        message: Some("scheduled restart".to_string()),
    };

    let json = serde_json::to_string(&event).expect("event json");

    assert!(json.contains(r#""event_type":"task_restarted""#));
    assert!(json.contains(r#""status_before":"healthy""#));
    assert!(json.contains(r#""status_after":"starting""#));
    assert!(json.contains(r#""reason":"cron""#));
}
