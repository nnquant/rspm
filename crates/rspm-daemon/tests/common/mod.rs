#![allow(dead_code)]

use std::path::Path;

pub fn toml_path(path: &Path) -> String {
    toml_string(&path.display().to_string().replace('\\', "/"))
}

pub fn toml_string(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => escaped.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => escaped.push(ch),
        }
    }
    escaped
}

pub fn task_command(unix_script: &str, windows_script: &str) -> String {
    if cfg!(windows) {
        format!(
            r#"cmd = "powershell.exe"
        args = ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", "{}"]"#,
            toml_string(windows_script)
        )
    } else {
        format!(
            r#"cmd = "sh"
        args = ["-c", "{}"]"#,
            toml_string(unix_script)
        )
    }
}

pub fn success_task_command() -> String {
    task_command("true", "exit 0")
}

pub fn failure_task_command() -> String {
    task_command("false", "exit 1")
}

pub fn sleep_task_command() -> String {
    task_command("sleep 30", "Start-Sleep -Seconds 30")
}

pub fn print_task_command(text: &str) -> String {
    task_command(
        &format!("printf '{}'", shell_single_quote(text)),
        &format!("[Console]::Out.Write('{}')", ps_single_quote(text)),
    )
}

pub fn print_stdout_stderr_task_command(stdout: &str, stderr: &str) -> String {
    task_command(
        &format!(
            "printf '{}'; printf '{}' >&2",
            shell_single_quote(stdout),
            shell_single_quote(stderr)
        ),
        &format!(
            "[Console]::Out.Write('{}'); [Console]::Error.Write('{}')",
            ps_single_quote(stdout),
            ps_single_quote(stderr)
        ),
    )
}

pub fn touch_command(path: &Path) -> String {
    if cfg!(windows) {
        format!(
            "powershell.exe -NoProfile -ExecutionPolicy Bypass -Command New-Item -ItemType File -Force -Path '{}' ^| Out-Null",
            ps_single_quote(&path.display().to_string())
        )
    } else {
        format!(
            "touch '{}'",
            shell_single_quote(&path.display().to_string())
        )
    }
}

pub fn sleep_then_touch_task_command(path: &Path) -> String {
    let unix_path = shell_single_quote(&path.display().to_string());
    let windows_path = ps_single_quote(&path.display().to_string());
    task_command(
        &format!("sleep 0.2; touch '{unix_path}'; sleep 30"),
        &format!(
            "Start-Sleep -Milliseconds 200; New-Item -ItemType File -Force -Path '{windows_path}' | Out-Null; Start-Sleep -Seconds 30"
        ),
    )
}

pub fn flaky_once_task_command(marker: &Path) -> String {
    let unix_marker = shell_single_quote(&marker.display().to_string());
    let windows_marker = ps_single_quote(&marker.display().to_string());
    task_command(
        &format!("if [ -f '{unix_marker}' ]; then sleep 30; else touch '{unix_marker}'; exit 1; fi"),
        &format!(
            "if (Test-Path '{windows_marker}') {{ Start-Sleep -Seconds 30 }} else {{ New-Item -ItemType File -Force -Path '{windows_marker}' | Out-Null; exit 1 }}"
        ),
    )
}

pub fn health_success_command() -> &'static str {
    if cfg!(windows) {
        "exit /B 0"
    } else {
        "true"
    }
}

pub fn health_failure_command() -> &'static str {
    if cfg!(windows) {
        "exit /B 1"
    } else {
        "false"
    }
}

fn shell_single_quote(value: &str) -> String {
    value.replace('\'', "'\\''")
}

fn ps_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}
