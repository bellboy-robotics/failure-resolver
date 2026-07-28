from __future__ import annotations

import asyncio
import json

import pytest
from websockets.asyncio.server import ServerConnection, serve

from robot_session import (
    CommandReceipt,
    RobotCommandNotSentError,
    RobotCommandOutcomeUnknownError,
    RobotCommandRejectedError,
    RobotSession,
    parse_flow_pointer,
    robot_websocket_url,
)


def flow(*, status: str = "paused", index: int = 10, command: str = "open_door"):
    return {
        "id": "flow-1",
        "filename": "room-101.yaml",
        "fid": "room-101",
        "flow_commit": "abc123",
        "name": "Open Door",
        "status": status,
        "current_action_index": index,
        "areas": [
            {
                "name": "Room",
                "items": [
                    {
                        "name": "Door",
                        "actions": [
                            {
                                "command": command,
                                "status": "aborted",
                                "action_index": index,
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_projects_a_guarded_flow_pointer():
    pointer = parse_flow_pointer(flow(), revision=7)

    assert pointer is not None
    assert pointer.revision == 7
    assert pointer.flow_id == "flow-1"
    assert pointer.action_index == 10
    assert pointer.action_command == "open_door"
    assert pointer.filename == "room-101.yaml"
    assert pointer.fid == "room-101"
    assert pointer.flow_commit == "abc123"


def test_projects_ready_program_counter_past_final_action_as_completed():
    completed_flow = flow(status="ready", index=0)
    completed_flow["current_action_index"] = 1

    pointer = parse_flow_pointer(
        completed_flow,
        revision=8,
        generation=3,
    )

    assert pointer is not None
    assert pointer.completed is True
    assert pointer.generation == 3
    assert pointer.action_index is None
    assert pointer.action_command is None


@pytest.mark.parametrize("sysid", ["../../host", "BILLIE_16", ""])
def test_robot_websocket_url_rejects_host_injection(sysid):
    with pytest.raises(ValueError):
        robot_websocket_url(sysid)


@pytest.mark.asyncio
async def test_correlates_command_response_and_receives_fresh_flow():
    headers = {}
    received = []

    async def handler(websocket: ServerConnection) -> None:
        assert websocket.request is not None
        headers.update(dict(websocket.request.headers.raw_items()))
        await websocket.send(json.dumps({"flow": flow()}))
        command = json.loads(await websocket.recv())
        received.append(command)
        await websocket.send(
            json.dumps(
                {
                    "response": {
                        "request_id": command["request_id"],
                        "result": {"accepted": True},
                        "error": None,
                    }
                }
            )
        )
        await websocket.send(
            json.dumps(
                {
                    "flow": flow(
                        status="in_progress",
                        index=11,
                        command="close_door",
                    )
                }
            )
        )
        await websocket.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        session = RobotSession(
            sysid="BILLIE-16",
            cf_access_client_id="client-id",
            cf_access_client_secret="client-secret",
            reconnect_initial_seconds=0.01,
            reconnect_max_seconds=0.02,
        )
        session.url = f"ws://127.0.0.1:{port}"
        stop = asyncio.Event()
        runner = asyncio.create_task(session.run(stop))
        await session.wait_connected(1)

        while session.latest_flow is None:
            await asyncio.sleep(0)
        revision = session.latest_flow.revision
        result = await session.request_command(
            "fold",
            {"wait": True},
            timeout_seconds=1,
        )
        advanced = await session.wait_for_flow(
            lambda pointer: pointer is not None and pointer.action_index == 11,
            after_revision=revision,
            timeout_seconds=1,
        )

        assert isinstance(result, CommandReceipt)
        assert result.result == {"accepted": True}
        assert advanced is not None
        assert advanced.status == "in_progress"
        assert received[0]["command"] == "fold"
        assert received[0]["arguments"] == {"wait": True}
        assert received[0]["sysid"] == "BILLIE-16"
        assert headers["CF-Access-Client-Id"] == "client-id"
        assert headers["CF-Access-Client-Secret"] == "client-secret"

        stop.set()
        await runner


@pytest.mark.asyncio
async def test_explicit_robot_error_is_rejected_but_timeout_is_unknown():
    calls = 0

    async def handler(websocket: ServerConnection) -> None:
        nonlocal calls
        while True:
            command = json.loads(await websocket.recv())
            calls += 1
            if calls == 1:
                await websocket.send(
                    json.dumps(
                        {
                            "response": {
                                "request_id": command["request_id"],
                                "result": None,
                                "error": "arm refused command",
                            }
                        }
                    )
                )

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        session = RobotSession(
            sysid="BILLIE-16",
            cf_access_client_id="client-id",
            cf_access_client_secret="client-secret",
        )
        session.url = f"ws://127.0.0.1:{port}"
        stop = asyncio.Event()
        runner = asyncio.create_task(session.run(stop))
        await session.wait_connected(1)

        with pytest.raises(RobotCommandRejectedError, match="arm refused"):
            await session.request_command("fold", {}, timeout_seconds=1)
        with pytest.raises(
            RobotCommandOutcomeUnknownError,
            match="timed out",
        ):
            await session.request_command("fold", {}, timeout_seconds=0.01)

        stop.set()
        await runner


@pytest.mark.asyncio
async def test_reconnect_invalidates_flow_and_commands_generation():
    session = RobotSession(
        sysid="BILLIE-16",
        cf_access_client_id="client-id",
        cf_access_client_secret="client-secret",
    )
    first_socket = object()
    await session._activate_connection(first_socket)
    first_generation = session.generation
    await session._handle_message(
        json.dumps(
            {
                "commands": {
                    "fold": {
                        "name": "fold",
                        "inputs": [],
                    }
                },
                "flow": flow(),
            }
        )
    )

    assert session.latest_flow is not None
    assert session.latest_commands_frame is not None
    assert session.latest_commands_frame.generation == first_generation

    await session._invalidate_connection()
    await session._activate_connection(object())

    assert session.generation == first_generation + 1
    assert session.latest_flow is None
    assert session.latest_commands_frame is None
    with pytest.raises(RobotCommandNotSentError):
        await session.wait_for_commands(
            generation=first_generation,
            timeout_seconds=0.01,
        )
