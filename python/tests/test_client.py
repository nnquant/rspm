import json
import asyncio
import socketserver
import threading

import pytest

from rspm import RspmClient, TaskInfo
from rspm.aio import AsyncRspmClient


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
