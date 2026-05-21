use chrono::{DateTime, FixedOffset, Local, Utc};
use chrono_tz::Tz;

pub const DEFAULT_DISPLAY_TIMEZONE: &str = "local";
pub const DISPLAY_TIME_FORMAT: &str = "%m-%d %H:%M:%S%:z";
pub const DISPLAY_TABLE_TIME_FORMAT: &str = "%m-%d %H:%M:%S";

pub fn format_display_time(time: &DateTime<Utc>, timezone: Option<&str>) -> String {
    format_display_time_with_pattern(time, timezone, DISPLAY_TIME_FORMAT)
}

pub fn format_display_table_time(time: &DateTime<Utc>, timezone: Option<&str>) -> String {
    format_display_time_with_pattern(time, timezone, DISPLAY_TABLE_TIME_FORMAT)
}

pub fn is_valid_display_timezone(timezone: &str) -> bool {
    let timezone = timezone.trim();
    timezone.is_empty()
        || timezone.eq_ignore_ascii_case(DEFAULT_DISPLAY_TIMEZONE)
        || timezone.parse::<Tz>().is_ok()
        || parse_utc_offset(timezone).is_some()
}

fn parse_utc_offset(value: &str) -> Option<FixedOffset> {
    let value = value
        .strip_prefix("UTC")
        .or_else(|| value.strip_prefix("GMT"))?;
    if value.is_empty() {
        return FixedOffset::east_opt(0);
    }

    let sign = match &value[..1] {
        "+" => 1,
        "-" => -1,
        _ => return None,
    };
    let rest = &value[1..];
    let (hours, minutes) = if let Some((hours, minutes)) = rest.split_once(':') {
        (hours.parse::<i32>().ok()?, minutes.parse::<i32>().ok()?)
    } else {
        (rest.parse::<i32>().ok()?, 0)
    };
    FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))
}

fn format_display_time_with_pattern(
    time: &DateTime<Utc>,
    timezone: Option<&str>,
    pattern: &str,
) -> String {
    let timezone = timezone.unwrap_or(DEFAULT_DISPLAY_TIMEZONE).trim();
    if timezone.is_empty() || timezone.eq_ignore_ascii_case(DEFAULT_DISPLAY_TIMEZONE) {
        return time.with_timezone(&Local).format(pattern).to_string();
    }

    if let Ok(tz) = timezone.parse::<Tz>() {
        return time.with_timezone(&tz).format(pattern).to_string();
    }

    if let Some(offset) = parse_utc_offset(timezone) {
        return time.with_timezone(&offset).format(pattern).to_string();
    }

    time.format(pattern).to_string()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn formats_display_time_in_iana_timezone() {
        let time = Utc.with_ymd_and_hms(2026, 5, 18, 8, 30, 0).unwrap();

        assert_eq!(
            format_display_time(&time, Some("Asia/Shanghai")),
            "05-18 16:30:00+08:00"
        );
    }

    #[test]
    fn formats_display_table_time_without_offset() {
        let time = Utc.with_ymd_and_hms(2026, 5, 18, 8, 30, 0).unwrap();

        assert_eq!(
            format_display_table_time(&time, Some("Asia/Shanghai")),
            "05-18 16:30:00"
        );
    }

    #[test]
    fn validates_display_timezone_names_and_offsets() {
        assert!(is_valid_display_timezone("local"));
        assert!(is_valid_display_timezone("America/New_York"));
        assert!(is_valid_display_timezone("UTC+08:00"));
        assert!(!is_valid_display_timezone("Mars/Base"));
    }
}
