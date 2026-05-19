"""Synchronous rspm client."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import socket
import time
from typing import Any


@dataclass(frozen=True)
class TaskInfo:
    """Structured task state returned by rspmd."""

    name: str
    task_id: int = 0
    run_mode: str = ""
    pid: int | None = None
    status: str = "defined"
    health: str | None = None
    started_at: str | None = None
    stopped_at: str | None = None
    uptime_ms: int | None = None
    cpu_percent: float | None = None
    memory_bytes: int | None = None
    restart_count: int = 0
    last_exit_code: int | None = None
    cwd: str | None = None
    cmd: str = ""
    dependencies: list[str] = field(default_factory=list)
    dependents: list[str] = field(default_factory=list)
    schedule_state: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskInfo":
        """Create :class:`TaskInfo` from a JSON-RPC result item."""

        return cls(
            task_id=payload.get("task_id", 0),
            name=payload["name"],
            run_mode=payload.get("run_mode", ""),
            pid=payload.get("pid"),
            status=payload.get("status", "defined"),
            health=payload.get("health"),
            started_at=payload.get("started_at"),
            stopped_at=payload.get("stopped_at"),
            uptime_ms=payload.get("uptime_ms"),
            cpu_percent=payload.get("cpu_percent"),
            memory_bytes=payload.get("memory_bytes"),
            restart_count=payload.get("restart_count", 0),
            last_exit_code=payload.get("last_exit_code"),
            cwd=payload.get("cwd"),
            cmd=payload.get("cmd", ""),
            dependencies=list(payload.get("dependencies", [])),
            dependents=list(payload.get("dependents", [])),
            schedule_state=payload.get("schedule_state"),
        )


class RspmError(RuntimeError):
    """Raised when rspmd returns a JSON-RPC error response."""


@dataclass
class RspmClient:
    """Build and send rspm JSON-RPC requests.

    :param endpoint: Local rspm endpoint, such as ``local://default``.
    :type endpoint: str
    """

    endpoint: str = "local://default"
    _next_id: int = field(default=1, init=False)
    _token: str | None = field(default=None, init=False, repr=False)

    @classmethod
    def connect_default(cls) -> "RspmClient":
        """Create a client for the default local rspm endpoint.

        :returns: Configured client.
        :rtype: RspmClient
        """

        return cls.connect_tcp()

    @classmethod
    def connect_tcp(cls, host: str = "127.0.0.1", port: int = 27691) -> "RspmClient":
        """Create a client connected to rspmd TCP fallback transport.

        :param host: rspmd host.
        :type host: str
        :param port: rspmd port.
        :type port: int
        :returns: TCP-capable client.
        :rtype: RspmClient
        """

        return cls(f"tcp://{host}:{port}")

    def build_request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Build a JSON-RPC request payload.

        :param method: RPC method name.
        :type method: str
        :param params: RPC parameters.
        :type params: dict[str, Any] | None
        :returns: JSON-serializable request payload.
        :rtype: dict[str, Any]
        """

        params = dict(params or {})
        if self._token is not None:
            params["token"] = self._token
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id,
            "method": method,
            "params": params,
        }
        self._next_id += 1
        return request

    def with_token(self, token: str) -> "RspmClient":
        """Attach an authentication token to subsequent JSON-RPC requests."""

        self._token = token
        return self

    def start(self, task: str) -> dict[str, Any]:
        """Build a request to start a task."""

        return self._task_request("task.start", task)

    def stop(self, task: str) -> dict[str, Any]:
        """Build a request to stop a task."""

        return self._task_request("task.stop", task)

    def restart(self, task: str) -> dict[str, Any]:
        """Build a request to restart a task."""

        return self._task_request("task.restart", task)

    def reload(self, task: str) -> dict[str, Any]:
        """Build or send a request to reload a task."""

        return self._task_request("task.reload", task)

    def describe_task(self, task: str) -> dict[str, Any]:
        """Build or send a request to describe a task."""

        return self._task_request("task.describe", task)

    def list_tasks(self) -> dict[str, Any]:
        """Build a request to list tasks."""

        response = self._request_or_send("task.list")
        if self.endpoint.startswith("tcp://"):
            return [TaskInfo.from_dict(item) for item in _result(response)]
        return response

    def logs(self, task: str) -> dict[str, Any]:
        """Build or send a request to read task logs."""

        response = self._request_or_send("task.logs", {"task": task})
        if self.endpoint.startswith("tcp://"):
            return _result(response)
        return response

    def tail_logs(self, task: str) -> dict[str, Any]:
        """Read task logs through the same RPC path used by ``logs``."""

        return self.logs(task)

    def logs_all(self) -> dict[str, str] | dict[str, Any]:
        """Read logs for all tasks through the configured daemon transport."""

        if not self.endpoint.startswith("tcp://"):
            return self.build_request("task.list")

        tasks = self.list_tasks()
        return {task.name: self.logs(task.name) for task in tasks}

    def events(self) -> dict[str, Any]:
        """Build or send a request to list daemon events."""

        response = self._request_or_send("event.list")
        if self.endpoint.startswith("tcp://"):
            return _result(response)
        return response

    def watch_events(self, task: str | None = None, poll_interval: float = 1.0) -> Any:
        """Poll rspmd events and yield newly observed events."""

        seen: set[str] = set()
        while True:
            events = self.events()
            if isinstance(events, dict):
                yield events
                return
            for event in events:
                key = json.dumps(event, sort_keys=True)
                if key in seen:
                    continue
                seen.add(key)
                if task is None or event.get("task") == task:
                    yield event
            time.sleep(poll_interval)

    def apply_file(self, path: str | Path) -> dict[str, Any]:
        """Apply a TOML configuration file through rspmd."""

        toml_text = Path(path).read_text()
        response = self._request_or_send("config.apply", {"toml": toml_text})
        if self.endpoint.startswith("tcp://"):
            return [TaskInfo.from_dict(item) for item in _result(response)]
        return response

    def wait_healthy(self, task: str, timeout: float = 30.0) -> TaskInfo | dict[str, Any]:
        """Wait until a task is healthy or online without a configured probe."""

        if not self.endpoint.startswith("tcp://"):
            return self.build_request("task.describe", {"task": task})

        deadline = time.monotonic() + timeout
        while True:
            info = self.describe_task(task)
            if isinstance(info, TaskInfo) and (
                info.status == "healthy" or (info.pid is not None and info.health is None)
            ):
                return info
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for task [{task}] to become healthy")
            time.sleep(0.1)

    def _task_request(self, method: str, task: str) -> dict[str, Any]:
        response = self._request_or_send(method, {"task": task})
        if self.endpoint.startswith("tcp://"):
            return TaskInfo.from_dict(_result(response))
        return response

    def _request_or_send(
        self, method: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        request = self.build_request(method, params)
        if self.endpoint.startswith("tcp://"):
            return self.send_request(request)
        return request

    def send_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Send a JSON-RPC request to the configured endpoint.

        :param request: JSON-RPC request payload.
        :type request: dict[str, Any]
        :returns: JSON-RPC response payload.
        :rtype: dict[str, Any]
        :raises ValueError: If endpoint scheme is unsupported.
        """

        if not self.endpoint.startswith("tcp://"):
            raise ValueError(f"unsupported endpoint [{self.endpoint}]")

        host_port = self.endpoint.removeprefix("tcp://")
        host, port_text = host_port.rsplit(":", 1)
        with socket.create_connection((host, int(port_text)), timeout=5) as sock:
            payload = json.dumps(request).encode() + b"\n"
            sock.sendall(payload)
            response = _read_json_line(sock)
        return response


def _read_json_line(sock: socket.socket) -> dict[str, Any]:
    chunks: list[bytes] = []
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        if chunk == b"\n":
            break
        chunks.append(chunk)
    if not chunks:
        raise ConnectionError("rspmd returned empty response")
    return json.loads(b"".join(chunks).decode())


def _result(response: dict[str, Any]) -> Any:
    error = response.get("error")
    if error is not None:
        raise RspmError(f"rspmd error [{error.get('code')}]: {error.get('message')}")
    return response.get("result")
