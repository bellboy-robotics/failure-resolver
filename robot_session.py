"""Correlated, fail-closed command sessions for one Billie robot.

The robot devices WebSocket is both the command channel and the authoritative
live Flow feed.  A command acknowledgement proves only that Brain accepted (or
rejected) a request.  Recovery success is decided separately from fresh Flow
frames.
"""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from websockets.asyncio.client import connect


_SYSID_PATTERN = re.compile(r"^[A-Za-z0-9-]{1,64}$")
_STOPPED_ACTION_STATUSES = ("failed", "aborted", "in_progress")
_COMPLETED_FLOW_STATUSES = frozenset(
    ("ready", "completed", "complete", "finished", "succeeded", "success")
)
_FLOW_HISTORY_LIMIT = 512


class RobotSessionError(RuntimeError):
    """Base class for bounded robot-session errors."""


class RobotCommandNotSentError(RobotSessionError):
    """The session was known disconnected before a command was dispatched."""


class RobotCommandRejectedError(RobotSessionError):
    """Brain returned a correlated explicit error."""


class RobotCommandOutcomeUnknownError(RobotSessionError):
    """The command may have been received, but no trustworthy result exists."""


@dataclass(frozen=True)
class FlowPointer:
    revision: int
    flow_id: str
    status: str
    action_index: int | None
    action_command: str | None
    filename: str | None
    fid: str | None
    flow_commit: str | None
    raw: Mapping[str, Any]
    generation: int = 0
    completed: bool = False


@dataclass(frozen=True)
class CommandsFrame:
    revision: int
    generation: int
    commands: Mapping[str, Any]


@dataclass(frozen=True)
class CommandReceipt:
    result: Any
    generation: int
    flow_revision_at_correlated_ack: int


@dataclass(frozen=True)
class _FlowFrame:
    revision: int
    generation: int
    pointer: FlowPointer | None


def robot_websocket_url(sysid: str) -> str:
    canonical = sysid.strip()
    if not _SYSID_PATTERN.fullmatch(canonical):
        raise ValueError("robot sysid must be a safe hostname label")
    return f"wss://{canonical.lower()}.ws.devices.bellboy.co/"


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _flow_actions(flow: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    actions: list[Mapping[str, Any]] = []
    areas = flow.get("areas")
    if not isinstance(areas, list):
        return actions
    for area in areas:
        if not isinstance(area, Mapping):
            continue
        items = area.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            raw_actions = item.get("actions")
            if not isinstance(raw_actions, list):
                continue
            actions.extend(
                action for action in raw_actions if isinstance(action, Mapping)
            )
    return actions


def parse_flow_pointer(
    flow: Any,
    *,
    revision: int,
    generation: int = 0,
) -> FlowPointer | None:
    """Project only the identity needed for guarded recovery."""

    if not isinstance(flow, Mapping):
        return None
    flow_id = next(
        (
            value
            for value in (
                _text(flow.get("id")),
                _text(flow.get("filename")),
                _text(flow.get("fid")),
            )
            if value is not None
        ),
        None,
    )
    status = _text(flow.get("status"))
    if flow_id is None or status is None:
        return None

    actions = _flow_actions(flow)
    current_index = _integer(flow.get("current_action_index"))
    if (
        current_index == len(actions)
        and status.casefold() in _COMPLETED_FLOW_STATUSES
    ):
        return FlowPointer(
            revision=revision,
            flow_id=flow_id,
            status=status,
            action_index=None,
            action_command=None,
            filename=_text(flow.get("filename")),
            fid=_text(flow.get("fid")),
            flow_commit=_text(flow.get("flow_commit")),
            raw=dict(flow),
            generation=generation,
            completed=True,
        )

    current: Mapping[str, Any] | None = None
    if current_index is not None:
        current = next(
            (
                action
                for action in actions
                if _integer(action.get("action_index")) == current_index
            ),
            None,
        )
    if current is None:
        current = next(
            (
                action
                for stopped_status in _STOPPED_ACTION_STATUSES
                for action in actions
                if _text(action.get("status")) == stopped_status
            ),
            None,
        )
    if current is None:
        return None

    action_index = _integer(current.get("action_index"))
    action_command = _text(current.get("command"))
    if action_index is None or action_index < 0 or action_command is None:
        return None

    return FlowPointer(
        revision=revision,
        flow_id=flow_id,
        status=status,
        action_index=action_index,
        action_command=action_command,
        filename=_text(flow.get("filename")),
        fid=_text(flow.get("fid")),
        flow_commit=_text(flow.get("flow_commit")),
        raw=dict(flow),
        generation=generation,
    )


@dataclass
class _PendingRequest:
    future: asyncio.Future[Any]


ConnectFactory = Callable[..., Any]


class RobotSession:
    """One reconnecting duplex WebSocket with request correlation."""

    def __init__(
        self,
        *,
        sysid: str,
        cf_access_client_id: str,
        cf_access_client_secret: str,
        open_timeout_seconds: float = 15.0,
        ping_interval_seconds: float = 20.0,
        ping_timeout_seconds: float = 20.0,
        close_timeout_seconds: float = 5.0,
        max_message_bytes: int = 2 * 1024 * 1024,
        reconnect_initial_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        connect_factory: ConnectFactory = connect,
    ) -> None:
        self.sysid = sysid.strip().upper()
        self.url = robot_websocket_url(self.sysid)
        if not cf_access_client_id.strip() or not cf_access_client_secret.strip():
            raise ValueError("Cloudflare Access service-token credentials are required")
        self._client_id = cf_access_client_id
        self._client_secret = cf_access_client_secret
        self._open_timeout_seconds = open_timeout_seconds
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._close_timeout_seconds = close_timeout_seconds
        self._max_message_bytes = max_message_bytes
        self._reconnect_initial_seconds = reconnect_initial_seconds
        self._reconnect_max_seconds = reconnect_max_seconds
        self._connect_factory = connect_factory
        self._websocket: Any | None = None
        self._connected = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._flow_condition = asyncio.Condition()
        self._commands_condition = asyncio.Condition()
        self._connection_generation = 0
        self._flow_revision = 0
        self._latest_flow: FlowPointer | None = None
        self._flow_history: deque[_FlowFrame] = deque(
            maxlen=_FLOW_HISTORY_LIMIT
        )
        self._commands_revision = 0
        self._latest_commands_frame: CommandsFrame | None = None
        self._pending: dict[str, _PendingRequest] = {}

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def latest_flow(self) -> FlowPointer | None:
        return self._latest_flow

    @property
    def latest_commands(self) -> Mapping[str, Any] | None:
        frame = self._latest_commands_frame
        return frame.commands if frame is not None else None

    @property
    def latest_commands_frame(self) -> CommandsFrame | None:
        return self._latest_commands_frame

    @property
    def generation(self) -> int:
        return self._connection_generation

    async def wait_connected(self, timeout_seconds: float) -> None:
        try:
            await asyncio.wait_for(
                self._connected.wait(),
                timeout=max(0.001, timeout_seconds),
            )
        except TimeoutError as error:
            raise RobotCommandNotSentError(
                "robot command channel is not connected"
            ) from error

    async def run(self, stop_event: asyncio.Event) -> None:
        reconnect_delay = self._reconnect_initial_seconds
        while not stop_event.is_set():
            websocket: Any | None = None
            try:
                async with self._connect_factory(
                    self.url,
                    additional_headers={
                        "CF-Access-Client-Id": self._client_id,
                        "CF-Access-Client-Secret": self._client_secret,
                    },
                    open_timeout=self._open_timeout_seconds,
                    ping_interval=self._ping_interval_seconds,
                    ping_timeout=self._ping_timeout_seconds,
                    close_timeout=self._close_timeout_seconds,
                    max_size=self._max_message_bytes,
                    proxy=None,
                ) as websocket:
                    await self._activate_connection(websocket)
                    reconnect_delay = self._reconnect_initial_seconds
                    while not stop_event.is_set():
                        receive_task = asyncio.create_task(websocket.recv())
                        stop_task = asyncio.create_task(stop_event.wait())
                        done, pending = await asyncio.wait(
                            {receive_task, stop_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for task in pending:
                            task.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        if stop_task in done and stop_task.result():
                            return
                        await self._handle_message(receive_task.result())
            except asyncio.CancelledError:
                raise
            except Exception:
                if stop_event.is_set():
                    return
            finally:
                if self._websocket is websocket:
                    await self._invalidate_connection()
                self._reject_pending_unknown(
                    "robot connection closed before the command outcome was known"
                )

            if await _wait_or_stop(stop_event, reconnect_delay):
                return
            reconnect_delay = min(
                reconnect_delay * 2,
                self._reconnect_max_seconds,
            )

    async def _activate_connection(self, websocket: Any) -> None:
        self._websocket = websocket
        self._connection_generation += 1
        self._latest_flow = None
        self._latest_commands_frame = None
        self._connected.set()
        async with self._flow_condition:
            self._flow_condition.notify_all()
        async with self._commands_condition:
            self._commands_condition.notify_all()

    async def _invalidate_connection(self) -> None:
        self._websocket = None
        self._connected.clear()
        self._latest_flow = None
        self._latest_commands_frame = None
        async with self._flow_condition:
            self._flow_condition.notify_all()
        async with self._commands_condition:
            self._commands_condition.notify_all()

    async def request_command(
        self,
        command: str,
        arguments: Mapping[str, Any],
        *,
        timeout_seconds: float,
    ) -> Any:
        websocket = self._websocket
        generation = self._connection_generation
        if not self.connected or websocket is None:
            raise RobotCommandNotSentError(
                "robot command channel is not connected"
            )
        if not isinstance(command, str) or not command.strip():
            raise ValueError("robot command must be non-empty text")
        if not isinstance(arguments, Mapping):
            raise ValueError("robot command arguments must be an object")

        request_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = _PendingRequest(future=future)
        payload = {
            "command": command.strip(),
            "arguments": dict(arguments),
            "request_id": request_id,
            "sysid": self.sysid,
            "sender": {
                "name": "failure-resolver",
                "email": None,
                "image": None,
            },
        }
        try:
            async with self._send_lock:
                if (
                    self._websocket is not websocket
                    or not self.connected
                    or self._connection_generation != generation
                ):
                    raise RobotCommandNotSentError(
                        "robot command channel changed before dispatch"
                    )
                await websocket.send(
                    json.dumps(payload, separators=(",", ":"), allow_nan=False)
                )
        except RobotCommandNotSentError:
            self._pending.pop(request_id, None)
            raise
        except Exception as error:
            self._pending.pop(request_id, None)
            raise RobotCommandOutcomeUnknownError(
                "robot command dispatch failed; its outcome is unknown"
            ) from error

        try:
            receipt = await asyncio.wait_for(
                future,
                timeout=max(0.001, timeout_seconds),
            )
            if (
                not self.connected
                or self._connection_generation != generation
            ):
                raise RobotCommandOutcomeUnknownError(
                    "robot connection changed at command acknowledgement"
                )
            if not isinstance(receipt, CommandReceipt):
                raise RobotCommandOutcomeUnknownError(
                    "robot command acknowledgement was malformed"
                )
            return receipt
        except TimeoutError as error:
            self._pending.pop(request_id, None)
            raise RobotCommandOutcomeUnknownError(
                "timed out waiting for the robot command response"
            ) from error
        finally:
            self._pending.pop(request_id, None)

    async def wait_for_commands(
        self,
        *,
        generation: int,
        timeout_seconds: float,
    ) -> CommandsFrame:
        def current_frame_available() -> bool:
            if (
                not self.connected
                or self._connection_generation != generation
            ):
                raise RobotCommandNotSentError(
                    "robot connection changed before a fresh commands frame"
                )
            frame = self._latest_commands_frame
            return frame is not None and frame.generation == generation

        async with self._commands_condition:
            if current_frame_available():
                frame = self._latest_commands_frame
                assert frame is not None
                return frame
            try:
                await asyncio.wait_for(
                    self._commands_condition.wait_for(
                        current_frame_available
                    ),
                    timeout=max(0.001, timeout_seconds),
                )
            except TimeoutError as error:
                raise RobotCommandNotSentError(
                    "timed out waiting for a fresh commands frame"
                ) from error
            frame = self._latest_commands_frame
            if frame is None or frame.generation != generation:
                raise RobotCommandNotSentError(
                    "fresh commands frame became unavailable"
                )
            return frame

    async def wait_for_flow(
        self,
        predicate: Callable[[FlowPointer | None], bool],
        *,
        after_revision: int,
        timeout_seconds: float,
        generation: int | None = None,
    ) -> FlowPointer | None:
        expected_generation = (
            self._connection_generation
            if generation is None
            else generation
        )
        cursor = after_revision
        selected: FlowPointer | None = None
        selected_set = False

        def changed_and_matches() -> bool:
            nonlocal cursor, selected, selected_set
            if (
                not self.connected
                or self._connection_generation != expected_generation
            ):
                raise RobotCommandOutcomeUnknownError(
                    "robot connection changed while waiting for Flow"
                )
            for frame in self._flow_history:
                if (
                    frame.revision <= cursor
                    or frame.generation != expected_generation
                ):
                    continue
                cursor = frame.revision
                if predicate(frame.pointer):
                    selected = frame.pointer
                    selected_set = True
                    return True
            return False

        async with self._flow_condition:
            if changed_and_matches():
                return selected
            try:
                await asyncio.wait_for(
                    self._flow_condition.wait_for(changed_and_matches),
                    timeout=max(0.001, timeout_seconds),
                )
            except TimeoutError as error:
                raise RobotCommandOutcomeUnknownError(
                    "timed out waiting for a fresh Flow outcome"
                ) from error
            if not selected_set:
                raise RobotCommandOutcomeUnknownError(
                    "fresh Flow outcome was not retained"
                )
            return selected

    async def _handle_message(self, message: str | bytes) -> None:
        if isinstance(message, bytes):
            try:
                message = message.decode("utf-8")
            except UnicodeDecodeError:
                return
        try:
            payload = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(payload, Mapping):
            return

        response = payload.get("response")
        if isinstance(response, Mapping):
            self._settle_response(response)

        if isinstance(payload.get("commands"), Mapping):
            async with self._commands_condition:
                self._commands_revision += 1
                self._latest_commands_frame = CommandsFrame(
                    revision=self._commands_revision,
                    generation=self._connection_generation,
                    commands=dict(payload["commands"]),
                )
                self._commands_condition.notify_all()

        if "flow" in payload:
            async with self._flow_condition:
                self._flow_revision += 1
                self._latest_flow = parse_flow_pointer(
                    payload.get("flow"),
                    revision=self._flow_revision,
                    generation=self._connection_generation,
                )
                self._flow_history.append(
                    _FlowFrame(
                        revision=self._flow_revision,
                        generation=self._connection_generation,
                        pointer=self._latest_flow,
                    )
                )
                self._flow_condition.notify_all()

    def _settle_response(self, response: Mapping[str, Any]) -> None:
        request_id = _text(response.get("request_id"))
        if request_id is None:
            return
        pending = self._pending.get(request_id)
        if pending is None or pending.future.done():
            return
        if "error" not in response:
            pending.future.set_exception(
                RobotCommandOutcomeUnknownError(
                    "robot returned a malformed correlated response"
                )
            )
            return
        remote_error = response.get("error")
        if remote_error is None:
            pending.future.set_result(
                CommandReceipt(
                    result=response.get("result"),
                    generation=self._connection_generation,
                    flow_revision_at_correlated_ack=self._flow_revision,
                )
            )
            return
        if isinstance(remote_error, str) and remote_error.strip():
            pending.future.set_exception(
                RobotCommandRejectedError(remote_error.strip())
            )
            return
        pending.future.set_exception(
            RobotCommandOutcomeUnknownError(
                "robot returned an invalid correlated error response"
            )
        )

    def _reject_pending_unknown(self, message: str) -> None:
        for pending in list(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(
                    RobotCommandOutcomeUnknownError(message)
                )


async def _wait_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=max(0.001, seconds))
        return True
    except TimeoutError:
        return False
