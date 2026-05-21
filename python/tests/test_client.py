import json
import asyncio
import re
import socketserver
import threading

import pytest

from rspm import RspmClient, RspmSupervisor, TaskInfo
from rspm.aio import AsyncRspmClient
from rspm.render import format_merged_logs, format_prefixed_logs, format_task_table


ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(value: str) -> str:
    return ANSI_PATTERN.sub("", value)


def test_sync_client_builds_json_rpc_start_request() -> None:
    client = RspmClient("local://test")

    request = client.build_request("task.start", {"task": "master"})

    assert request["jsonrpc"] == "2.0"
    assert request["method"] == "task.start"
    assert request["params"] == {"task": "master"}
    assert isinstance(request["id"], int)


def test_sync_client_exposes_task_operations_as_request_payloads() -> None:
    client = RspmClient("local://test")

    assert client.start("master")["method"] == "task.start"
    assert client.stop("master")["method"] == "task.stop"
    assert client.restart("master")["method"] == "task.restart"
    assert client.list_tasks()["method"] == "task.list"


def test_sync_tcp_client_sends_json_line_request() -> None:
    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request = json.loads(self.rfile.readline().decode())
            assert request["method"] == "task.list"
            self.wfile.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": [{"name": "master", "status": "stopped"}],
                    }
                ).encode()
                + b"\n"
            )

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()
        host, port = server.server_address
        client = RspmClient.connect_tcp(host, port)

        response = client.list_tasks()

        thread.join(timeout=2)

    assert response == [TaskInfo(name="master", status="stopped")]


def test_sync_tcp_client_reads_aggregate_logs() -> None:
    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request = json.loads(self.rfile.readline().decode())
            if request["method"] == "task.list":
                result = [
                    {"name": "alpha", "status": "stopped"},
                    {"name": "beta", "status": "stopped"},
                ]
            else:
                result = f"{request['params']['task']}-log"
            self.wfile.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": result,
                    }
                ).encode()
                + b"\n"
            )

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(
            target=lambda: [server.handle_request() for _ in range(3)],
            daemon=True,
        )
        thread.start()
        host, port = server.server_address
        client = RspmClient.connect_tcp(host, port)

        response = client.logs_all()

        thread.join(timeout=2)

    assert response == {"alpha": "alpha-log", "beta": "beta-log"}


def test_sync_client_injects_configured_auth_token() -> None:
    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request = json.loads(self.rfile.readline().decode())
            self.wfile.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": request["params"].get("token"),
                    }
                ).encode()
                + b"\n"
            )

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()
        host, port = server.server_address
        client = RspmClient.connect_tcp(host, port).with_token("secret-token")

        response = client.send_request(client.build_request("task.list"))

        thread.join(timeout=2)

    assert response["result"] == "secret-token"


def test_supervisor_builds_detached_daemon_command(tmp_path) -> None:
    supervisor = RspmSupervisor(
        host="127.0.0.1",
        port=39001,
        rspm_bin="/opt/rspm/bin/rspm",
        log_dir=tmp_path / "logs",
        state_dir=tmp_path / "state",
        socket_path=tmp_path / "run" / "rspmd.sock",
        token="secret-token",
    )

    command = supervisor.daemon_command("tasks.rspm.toml")

    assert supervisor.ownership == "detached"
    assert command == [
        "/opt/rspm/bin/rspm",
        "daemon",
        "run",
        "tasks.rspm.toml",
        "127.0.0.1:39001",
        str(tmp_path / "logs"),
        str(tmp_path / "state"),
        str(tmp_path / "run" / "rspmd.sock"),
        "--token",
        "secret-token",
    ]


def test_supervisor_reuses_running_daemon_without_spawning() -> None:
    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request = json.loads(self.rfile.readline().decode())
            assert request["method"] == "task.list"
            self.wfile.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": [{"name": "master", "status": "stopped"}],
                    }
                ).encode()
                + b"\n"
            )

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(
            target=lambda: [server.handle_request() for _ in range(2)],
            daemon=True,
        )
        thread.start()
        host, port = server.server_address
        supervisor = RspmSupervisor(host=host, port=port, rspm_bin="/missing/rspm")

        client = supervisor.ensure_daemon("missing-config.toml")
        response = client.list_tasks()

        thread.join(timeout=2)

    assert response == [TaskInfo(name="master", status="stopped")]


def test_render_task_table_matches_cli_style() -> None:
    output = format_task_table(
        [
            TaskInfo(
                name="market",
                task_id=1,
                run_mode="long",
                pid=42,
                status="online",
                health="ok",
                started_at="2026-05-20T01:02:03Z",
                uptime_ms=61_000,
                cpu_percent=80.0,
                memory_bytes=512 * 1024 * 1024,
                restart_count=3,
                schedule_state="start 05-20 09:30:00",
                display_timezone="Asia/Shanghai",
            )
        ]
    )

    assert "TASK_ID" in output
    assert "START_TIME" in output
    assert "market" in output
    assert "05-20 09:02:03" in output
    assert "\x1b[32monline" in output
    assert "\x1b[90mTimezone: Asia/Shanghai\x1b[0m" in output


def test_render_task_table_keeps_columns_aligned_for_long_task_name() -> None:
    output = strip_ansi(
        format_task_table(
            [
                TaskInfo(
                    name="ldc-ctp-bond-future-factors",
                    task_id=1,
                    run_mode="oneshot",
                    pid=2938490,
                    status="online",
                )
            ]
        )
    )
    header, row, *_ = output.splitlines()

    assert header.index("MODE") == row.index("oneshot")
    assert "ldc-ctp-bond-future-factors" in row
    assert "..." not in row


def test_render_prefixed_logs_keeps_terminal_styles() -> None:
    output = format_prefixed_logs("market", "\x1b[32mINFO\x1b[0m started\n")

    assert output == "market | \x1b[32mINFO\x1b[0m started\n"


def test_render_merged_logs_orders_timestamped_lines() -> None:
    output = format_merged_logs(
        [
            ("beta", "2026-05-20T01:00:02Z beta\n"),
            ("alpha", "2026-05-20T01:00:01Z alpha\n"),
        ]
    )

    assert output == (
        "alpha | 2026-05-20T01:00:01Z alpha\n"
        "beta | 2026-05-20T01:00:02Z beta\n"
    )


@pytest.mark.asyncio
async def test_async_client_context_manager_builds_requests() -> None:
    async with AsyncRspmClient("local://test") as client:
        request = await client.start("strategy")

    assert request["jsonrpc"] == "2.0"
    assert request["method"] == "task.start"
    assert request["params"] == {"task": "strategy"}


@pytest.mark.asyncio
async def test_async_tcp_client_sends_json_line_request() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request = json.loads((await reader.readline()).decode())
        assert request["method"] == "task.list"
        writer.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": [{"name": "async-master", "status": "stopped"}],
                }
            ).encode()
            + b"\n"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()
    try:
        async with AsyncRspmClient.connect_tcp(host, port) as client:
            response = await client.list_tasks()
    finally:
        server.close()
        await server.wait_closed()

    assert response == [TaskInfo(name="async-master", status="stopped")]


@pytest.mark.asyncio
async def test_async_tcp_client_reads_aggregate_logs() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request = json.loads((await reader.readline()).decode())
        if request["method"] == "task.list":
            result = [
                {"name": "alpha", "status": "stopped"},
                {"name": "beta", "status": "stopped"},
            ]
        else:
            result = f"{request['params']['task']}-log"
        writer.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": result,
                }
            ).encode()
            + b"\n"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()
    try:
        async with AsyncRspmClient.connect_tcp(host, port) as client:
            response = await client.logs_all()
    finally:
        server.close()
        await server.wait_closed()

    assert response == {"alpha": "alpha-log", "beta": "beta-log"}


@pytest.mark.asyncio
async def test_async_client_injects_configured_auth_token() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request = json.loads((await reader.readline()).decode())
        writer.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": request["params"].get("token"),
                }
            ).encode()
            + b"\n"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()
    try:
        async with AsyncRspmClient.connect_tcp(host, port).with_token("secret-token") as client:
            response = await client.send_request(client._client.build_request("task.list"))
    finally:
        server.close()
        await server.wait_closed()

    assert response["result"] == "secret-token"
