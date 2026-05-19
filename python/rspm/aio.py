"""Asynchronous rspm client."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from rspm.client import RspmClient, TaskInfo, _result


class AsyncRspmClient:
    """Async wrapper for rspm JSON-RPC request construction."""

    def __init__(self, endpoint: str = "local://default") -> None:
        self._client = RspmClient(endpoint)

    @classmethod
    def connect_tcp(cls, host: str = "127.0.0.1", port: int = 27691) -> "AsyncRspmClient":
        """Create an async client for rspmd TCP fallback transport."""

        return cls(f"tcp://{host}:{port}")

    @classmethod
    def connect_default(cls) -> "AsyncRspmClient":
        """Create an async client for the default rspmd TCP endpoint."""

        return cls.connect_tcp()

    async def __aenter__(self) -> "AsyncRspmClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def with_token(self, token: str) -> "AsyncRspmClient":
        """Attach an authentication token to subsequent JSON-RPC requests."""

        self._client.with_token(token)
        return self

    async def start(self, task: str) -> dict[str, Any]:
        """Build a request to start a task."""

        return await self._task_request("task.start", task)

    async def stop(self, task: str) -> dict[str, Any]:
        """Build a request to stop a task."""

        return await self._task_request("task.stop", task)

    async def restart(self, task: str) -> dict[str, Any]:
        """Build a request to restart a task."""

        return await self._task_request("task.restart", task)

    async def reload(self, task: str) -> dict[str, Any]:
        """Build or send a request to reload a task."""

        return await self._task_request("task.reload", task)

    async def describe_task(self, task: str) -> dict[str, Any]:
        """Build or send a request to describe a task."""

        return await self._task_request("task.describe", task)

    async def list_tasks(self) -> dict[str, Any]:
        """Build a request to list tasks."""

        response = await self._request_or_send("task.list")
        if self._client.endpoint.startswith("tcp://"):
            return [TaskInfo.from_dict(item) for item in _result(response)]
        return response

    async def logs(self, task: str) -> dict[str, Any]:
        """Build or send a request to read task logs."""

        response = await self._request_or_send("task.logs", {"task": task})
        if self._client.endpoint.startswith("tcp://"):
            return _result(response)
        return response

    async def tail_logs(self, task: str) -> dict[str, Any]:
        """Read task logs through the same RPC path used by ``logs``."""

        return await self.logs(task)

    async def logs_all(self) -> dict[str, str] | dict[str, Any]:
        """Read logs for all tasks through the configured daemon transport."""

        if not self._client.endpoint.startswith("tcp://"):
            return self._client.build_request("task.list")

        tasks = await self.list_tasks()
        return {task.name: await self.logs(task.name) for task in tasks}

    async def events(self) -> dict[str, Any]:
        """Build or send a request to list daemon events."""

        response = await self._request_or_send("event.list")
        if self._client.endpoint.startswith("tcp://"):
            return _result(response)
        return response

    async def apply_file(self, path: str | Path) -> dict[str, Any]:
        """Apply a TOML configuration file through rspmd."""

        toml_text = Path(path).read_text()
        response = await self._request_or_send("config.apply", {"toml": toml_text})
        if self._client.endpoint.startswith("tcp://"):
            return [TaskInfo.from_dict(item) for item in _result(response)]
        return response

    async def wait_healthy(self, task: str, timeout: float = 30.0) -> TaskInfo | dict[str, Any]:
        """Wait until a task is healthy or online without a configured probe."""

        if not self._client.endpoint.startswith("tcp://"):
            return self._client.build_request("task.describe", {"task": task})

        deadline = time.monotonic() + timeout
        while True:
            info = await self.describe_task(task)
            if isinstance(info, TaskInfo) and (
                info.status == "healthy" or (info.pid is not None and info.health is None)
            ):
                return info
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for task [{task}] to become healthy")
            await asyncio.sleep(0.1)

    async def watch_events(
        self, task: str | None = None, poll_interval: float = 1.0
    ) -> Any:
        """Poll rspmd events and yield newly observed events."""

        seen: set[str] = set()
        while True:
            events = await self.events()
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
            await asyncio.sleep(poll_interval)

    async def _task_request(self, method: str, task: str) -> dict[str, Any]:
        response = await self._request_or_send(method, {"task": task})
        if self._client.endpoint.startswith("tcp://"):
            return TaskInfo.from_dict(_result(response))
        return response

    async def _request_or_send(
        self, method: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        request = self._client.build_request(method, params)
        if self._client.endpoint.startswith("tcp://"):
            return await self.send_request(request)
        return request

    async def send_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Send a JSON-RPC request to the configured endpoint."""

        if not self._client.endpoint.startswith("tcp://"):
            raise ValueError(f"unsupported endpoint [{self._client.endpoint}]")

        host_port = self._client.endpoint.removeprefix("tcp://")
        host, port_text = host_port.rsplit(":", 1)
        reader, writer = await asyncio.open_connection(host, int(port_text))
        writer.write(json.dumps(request).encode() + b"\n")
        await writer.drain()
        response = json.loads((await reader.readline()).decode())
        writer.close()
        await writer.wait_closed()
        return response
