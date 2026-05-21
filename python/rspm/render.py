"""Render rspm task tables and task logs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

from rspm.client import TaskInfo

TASK_NAME_WIDTH = 32


@dataclass(frozen=True)
class RenderLogOptions:
    """Filtering options used by log rendering helpers."""

    lines: int | None = None
    grep: str | None = None
    since: datetime | None = None


def format_task_table(tasks: list[TaskInfo]) -> str:
    """Render tasks using the same table style as the rspm CLI."""

    rows = [
        (
            f"{'TASK_ID':<8} {'NAME':<32} {'MODE':<10} {'PID':<8} {'STATUS':<12} "
            f"{'HEALTH':<8} {'RESTARTS':<8} {'UPTIME':<10} {'START_TIME':<15} "
            f"{'STOP_TIME':<15} {'CPU':<8} {'MEM':<8} NEXT"
        )
    ]
    for task in tasks:
        pid = str(task.pid) if task.pid is not None else "-"
        uptime = format_duration(task.uptime_ms) if task.uptime_ms is not None else "-"
        started = _format_task_time(task.started_at, task.display_timezone)
        stopped = _format_task_time(task.stopped_at, task.display_timezone)
        cpu_text = f"{task.cpu_percent:.1f}%" if task.cpu_percent is not None else "-"
        rows.append(
            f"{task.task_id:<8} {_fixed_width_cell(task.name, TASK_NAME_WIDTH)} "
            f"{_display_run_mode(task):<10} {pid:<8} "
            f"{_colored_status_cell(task.status)} {_colored_health_cell(task.health or '-')} "
            f"{_colored_restarts_cell(task.restart_count)} {uptime:<10} {started:<15} "
            f"{stopped:<15} {_colored_cpu_cell(cpu_text)} {_colored_memory_cell(task.memory_bytes)} "
            f"{task.schedule_state or '-'}"
        )
    rows.append(_colored_note_line(f"Timezone: {_table_display_timezone(tasks)}"))
    return "\n".join(rows) + "\n"


def format_prefixed_logs(
    task: str,
    logs: str,
    options: RenderLogOptions | None = None,
) -> str:
    """Render log lines with ``task | `` prefixes while preserving ANSI styles."""

    options = options or RenderLogOptions()
    output = "".join(f"{task} | {line}" for line in _selected_log_lines(logs, options))
    if logs and not logs.endswith("\n"):
        output += "\n"
    return output


def format_merged_logs(
    logs: list[tuple[str, str]],
    options: RenderLogOptions | None = None,
) -> str:
    """Render aggregate logs ordered by parseable RFC3339 timestamps."""

    options = options or RenderLogOptions()
    lines: list[tuple[datetime | None, int, str, str]] = []
    sequence = 0
    for task, text in logs:
        for line in _selected_log_lines(text, options):
            lines.append((_line_timestamp(line), sequence, task, line))
            sequence += 1
    lines.sort(key=lambda item: (item[0] is None, item[0] or datetime.max, item[1]))
    return "".join(f"{task} | {line}" for _, _, task, line in lines)


def format_duration(uptime_ms: int) -> str:
    """Format milliseconds as compact seconds, minutes, hours, or days."""

    seconds = uptime_ms // 1_000
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m{seconds % 60}s"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h{minutes % 60}m"
    days = hours // 24
    return f"{days}d{hours % 24}h"


def format_bytes(value: int) -> str:
    """Format bytes as B, KB, MB, or GB."""

    kb = 1024
    mb = 1024 * kb
    gb = 1024 * mb
    if value >= gb:
        return f"{value // gb}GB"
    if value >= mb:
        return f"{value // mb}MB"
    if value >= kb:
        return f"{value // kb}KB"
    return f"{value}B"


def _fixed_width_cell(value: str, width: int) -> str:
    if len(value) > width:
        value = value[: width - 3] + "..."
    return f"{value:<{width}}"


def _selected_log_lines(logs: str, options: RenderLogOptions) -> list[str]:
    lines = [
        line
        for line in logs.splitlines(keepends=True)
        if (options.grep is None or options.grep in line)
        and (options.since is None or (_line_timestamp(line) or datetime.min) >= options.since)
    ]
    if options.lines is not None:
        lines = lines[-options.lines :]
    return lines


def _line_timestamp(line: str) -> datetime | None:
    for token in line.split():
        stripped = token.strip("[](),")
        if stripped.endswith("Z"):
            stripped = stripped[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(stripped).astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _format_task_time(value: str | None, display_timezone: str | None) -> str:
    if value is None:
        return "-"
    parsed = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(parsed).astimezone(_display_tz(display_timezone))
    return dt.strftime("%m-%d %H:%M:%S")


def _display_tz(display_timezone: str | None) -> tzinfo:
    if display_timezone is None or display_timezone == "local":
        return datetime.now().astimezone().tzinfo or timezone.utc
    if display_timezone.startswith("UTC") or display_timezone.startswith("GMT"):
        suffix = display_timezone[3:]
        if not suffix:
            return timezone.utc
        sign = 1 if suffix[0] == "+" else -1
        hours, _, minutes = suffix[1:].partition(":")
        return timezone(sign * timedelta(hours=int(hours), minutes=int(minutes or 0)))
    return ZoneInfo(display_timezone)


def _display_run_mode(task: TaskInfo) -> str:
    return task.run_mode or "-"


def _table_display_timezone(tasks: list[TaskInfo]) -> str:
    for task in tasks:
        if task.display_timezone:
            return task.display_timezone
    return "local"


def _colored_status_cell(status: str) -> str:
    return _colorize_status(status, f"{status:<12}")


def _colorize_status(status: str, value: str) -> str:
    if status in {"online", "healthy"}:
        color = "32"
    elif status in {"unhealthy", "failed", "stopped"}:
        color = "31"
    elif status in {"starting", "scheduled", "waiting", "stopping"}:
        color = "33"
    elif status == "backoff":
        color = "35"
    else:
        color = "90"
    return f"\x1b[{color}m{value}\x1b[0m"


def _colored_health_cell(health: str) -> str:
    value = f"{health:<8}"
    if health == "ok":
        return f"\x1b[32m{value}\x1b[0m"
    if health == "fail":
        return f"\x1b[31m{value}\x1b[0m"
    if health == "-":
        return value
    return f"\x1b[33m{value}\x1b[0m"


def _colored_restarts_cell(restart_count: int) -> str:
    value = f"{restart_count:<8}"
    return f"\x1b[33m{value}\x1b[0m" if restart_count >= 3 else value


def _colored_cpu_cell(cpu: str) -> str:
    value = f"{cpu:<8}"
    try:
        high = float(cpu.rstrip("%")) >= 80.0
    except ValueError:
        high = False
    return f"\x1b[33m{value}\x1b[0m" if high else value


def _colored_memory_cell(memory_bytes: int | None) -> str:
    text = format_bytes(memory_bytes) if memory_bytes is not None else "-"
    value = f"{text:<8}"
    if memory_bytes is not None and memory_bytes >= 512 * 1024 * 1024:
        return f"\x1b[33m{value}\x1b[0m"
    return value


def _colored_note_line(value: str) -> str:
    return f"\x1b[90m{value}\x1b[0m"
