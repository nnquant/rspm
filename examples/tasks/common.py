"""Shared helpers for rspm simulated task examples."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import sys
from typing import Any


class ShutdownFlag:
    """
    Track process shutdown requested by operating-system signals.

    :param requested: Whether shutdown has been requested.
    :type requested: bool
    """

    def __init__(self) -> None:
        self.requested = False

    def request(self, signum: int, _frame: object) -> None:
        """
        Mark shutdown as requested.

        :param signum: Signal number received by the process.
        :type signum: int
        :param _frame: Python signal frame object.
        :type _frame: object
        """

        self.requested = True
        emit("signal_received", signum=signum)


def install_signal_handlers() -> ShutdownFlag:
    """
    Install SIGTERM/SIGINT handlers and return a shared shutdown flag.

    :returns: Mutable shutdown flag.
    :rtype: ShutdownFlag
    """

    flag = ShutdownFlag()
    signal.signal(signal.SIGTERM, flag.request)
    signal.signal(signal.SIGINT, flag.request)
    return flag


def emit(event: str, **fields: Any) -> None:
    """
    Print one structured JSON log line.

    :param event: Event name.
    :type event: str
    :param fields: Additional event fields.
    :type fields: dict[str, Any]
    """

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def touch_ready_file(path: str | None, task_name: str) -> None:
    """
    Create a simple readiness file for file health checks.

    :param path: Ready file path, or ``None`` to skip.
    :type path: str | None
    :param task_name: Task name written into the file.
    :type task_name: str
    """

    if path is None:
        return
    ready_path = Path(path)
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    ready_path.write_text(f"{task_name}\n", encoding="utf-8")


def remove_ready_file(path: str | None) -> None:
    """
    Remove a readiness file if it exists.

    :param path: Ready file path, or ``None`` to skip.
    :type path: str | None
    """

    if path is None:
        return
    try:
        Path(path).unlink()
    except FileNotFoundError:
        return


def exit_with_error(message: str) -> None:
    """
    Emit an error event and exit with status code 1.

    :param message: Error message.
    :type message: str
    """

    emit("task_error", message=message)
    raise SystemExit(1)
