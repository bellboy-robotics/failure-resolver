from __future__ import annotations

import asyncio
import copy
from typing import Any, Mapping, Sequence

import pytest

from recovery_executor import (
    RecoveryContractError,
    RecoveryAction,
    RecoveryCoordinator,
    RecoveryExecutionSettings,
    RecoveryExecutionState,
    SupabaseRecoveryDatabase,
    build_guarded_resume_arguments,
    classify_flow_outcome,
    parse_recovery_plan,
    parse_recovery_session,
    validate_corrections_against_commands,
)
from resolver import ResolverSettings
from robot_session import (
    CommandReceipt,
    CommandsFrame,
    FlowPointer,
    RobotCommandNotSentError,
    RobotCommandOutcomeUnknownError,
)


FAILURE_ID = "99e7f23d-64a7-4cd8-a0d8-e36154122f78"
SECOND_FAILURE_ID = "54d4b8dd-7d3a-4ab6-9dc8-36101cb694d1"
SESSION_ID = "11111111-2222-4333-8444-555555555555"
RUN_TOKENS = (
    "aaaaaaaa-1111-4111-8111-111111111111",
    "bbbbbbbb-2222-4222-8222-222222222222",
    "cccccccc-3333-4333-8333-333333333333",
)
MEMORY_ID = "5b5bbdc0-7c96-4bd8-9c6b-6ec989f3275e"
FLOW_ID = "flow-room-101"
ACTION_INDEX = 6
ACTION_COMMAND = "open_drawer"


def rerun_action() -> dict[str, Any]:
    return {
        "command": "$rerun",
        "title": "Retry",
        "arguments": {},
        "arguments_effective": {},
        "explicit_arguments": [],
        "retry_context": {
            "retried_action": {
                "command": ACTION_COMMAND,
                "actionIndex": ACTION_INDEX,
                "areaName": "Closet",
                "itemName": "Drawer",
            },
            "expected_next_action": {
                "command": "close_drawer",
                "actionIndex": ACTION_INDEX + 1,
                "areaName": "Closet",
                "itemName": "Drawer",
            },
        },
    }


def resume_action(rewind_steps: int) -> dict[str, Any]:
    arguments = {
        "mode": "retry_current" if rewind_steps == 0 else "rewind",
        "rewind_steps": rewind_steps,
        "current_action_index": ACTION_INDEX,
        "current_command": ACTION_COMMAND,
        "flow_id": FLOW_ID,
    }
    return {
        "command": "$resume_flow",
        "title": "Continue flow",
        "arguments": arguments,
        "arguments_effective": arguments,
        "explicit_arguments": list(arguments),
        "continuation_context": {
            "expected_arguments": arguments,
            "current_action": {
                "actionIndex": ACTION_INDEX,
                "command": ACTION_COMMAND,
                "areaName": "Closet",
                "itemName": "Drawer",
            },
            "target_action": {
                "actionIndex": ACTION_INDEX - rewind_steps,
                "command": "previous" if rewind_steps else ACTION_COMMAND,
                "areaName": "Entry" if rewind_steps else "Closet",
                "itemName": "Door" if rewind_steps else "Drawer",
            },
        },
    }


def correction_action(command: str = "fold") -> dict[str, Any]:
    return {
        "command": command,
        "title": command.title(),
        "arguments": {"wait": True, "speed": 60},
        "arguments_effective": {"wait": True, "speed": 60},
        "explicit_arguments": ["wait", "speed"],
    }


def suggestion(actions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "memory_id": MEMORY_ID,
        "summary": "Apply the stored fix.",
        "reason": "Exact failure match.",
        "confidence": 0.99,
        "actions": copy.deepcopy(list(actions)),
    }


def failure_row(
    *,
    actions: Sequence[Mapping[str, Any]] | None = None,
    failure_id: str = FAILURE_ID,
    recovery_session_id: str | None = None,
    recovery_status: str | None = None,
) -> dict[str, Any]:
    return {
        "failure_id": failure_id,
        "sysid": "BILLIE-16",
        "flow_id": FLOW_ID,
        "action_index": ACTION_INDEX,
        "action_command": ACTION_COMMAND,
        "analysis_status": "completed",
        "matcher_status": "solution_found",
        "resolver_suggestion": suggestion(
            actions or (correction_action(), rerun_action())
        ),
        "recovery_episode_key": "e" * 64,
        "recovery_session_id": recovery_session_id,
        "recovery_status": recovery_status,
    }


def pointer(
    revision: int,
    *,
    status: str = "paused",
    index: int = ACTION_INDEX,
    command: str = ACTION_COMMAND,
    flow_id: str = FLOW_ID,
) -> FlowPointer:
    actions = []
    for action_index in range(ACTION_INDEX + 2):
        if action_index == ACTION_INDEX:
            action_command = ACTION_COMMAND
            area_name = "Closet"
            item_name = "Drawer"
        elif action_index == ACTION_INDEX + 1:
            action_command = "close_drawer"
            area_name = "Closet"
            item_name = "Drawer"
        else:
            action_command = "previous"
            area_name = "Entry"
            item_name = "Door"
        actions.append(
            (
                area_name,
                item_name,
                {
                    "action_index": action_index,
                    "command": action_command,
                    "status": (
                        "in_progress"
                        if action_index == index
                        else "done"
                    ),
                },
            )
        )
    areas = []
    for area_name, item_name, action in actions:
        areas.append(
            {
                "name": area_name,
                "items": [
                    {
                        "name": item_name,
                        "actions": [action],
                    }
                ],
            }
        )
    return FlowPointer(
        revision=revision,
        flow_id=flow_id,
        status=status,
        action_index=index,
        action_command=command,
        filename=None,
        fid=None,
        flow_commit=None,
        raw={
            "id": flow_id,
            "status": status,
            "current_action_index": index,
            "areas": areas,
        },
        generation=1,
    )


class FakeRobotSession:
    def __init__(
        self,
        flow_updates: Sequence[FlowPointer | None],
        log: list[tuple[Any, ...]],
        *,
        command_error: Exception | None = None,
    ) -> None:
        self.connected = True
        self.generation = 1
        self.latest_flow: FlowPointer | None = pointer(1)
        self.latest_commands_frame: CommandsFrame | None = CommandsFrame(
            revision=1,
            generation=1,
            commands={
                "fold": {
                    "name": "fold",
                    "inputs": [
                        {
                            "name": "wait",
                            "type": "boolean",
                            "required": False,
                        },
                        {
                            "name": "speed",
                            "type": "float",
                            "required": False,
                            "minimum": 1,
                            "maximum": 100,
                        },
                    ],
                },
                "bump": {
                    "name": "bump",
                    "inputs": [],
                },
            },
        )
        self.flow_updates = list(flow_updates)
        self.log = log
        self.command_error = command_error
        self.commands: list[tuple[str, Mapping[str, Any]]] = []

    async def run(self, stop_event) -> None:
        await stop_event.wait()

    async def wait_connected(self, timeout_seconds: float) -> None:
        self.log.append(("wait_connected",))

    async def wait_for_commands(
        self,
        *,
        generation: int,
        timeout_seconds: float,
    ) -> CommandsFrame:
        self.log.append(("wait_commands", generation))
        assert self.latest_commands_frame is not None
        return self.latest_commands_frame

    async def request_command(
        self,
        command: str,
        arguments: Mapping[str, Any],
        *,
        timeout_seconds: float,
    ) -> CommandReceipt:
        self.log.append(("command", command, copy.deepcopy(dict(arguments))))
        self.commands.append((command, copy.deepcopy(dict(arguments))))
        if self.command_error is not None:
            error = self.command_error
            self.command_error = None
            raise error
        if command != "$resume_flow":
            result: Any = {"accepted": True}
        else:
            result = {
                "accepted": True,
                "mode": arguments["mode"],
                "rewind_steps": arguments["rewind_steps"],
                "previous_action_index": arguments[
                    "current_action_index"
                ],
                "target_action_index": (
                    arguments["current_action_index"]
                    - arguments["rewind_steps"]
                ),
                "current_command": arguments["current_command"],
                "flow_id": arguments["flow_id"],
            }
        return CommandReceipt(
            result=result,
            generation=self.generation,
            flow_revision_at_correlated_ack=(
                self.latest_flow.revision
                if self.latest_flow is not None
                else 0
            ),
        )

    async def wait_for_flow(
        self,
        predicate,
        *,
        after_revision: int,
        timeout_seconds: float,
        generation: int | None = None,
    ) -> FlowPointer | None:
        self.log.append(("wait_flow", after_revision))
        while self.flow_updates:
            candidate = self.flow_updates.pop(0)
            self.latest_flow = candidate
            if predicate(candidate):
                return candidate
        raise RobotCommandOutcomeUnknownError("no fresh Flow outcome")


class BlockingRobotSession(FakeRobotSession):
    def __init__(
        self,
        flow_updates: Sequence[FlowPointer | None],
        log: list[tuple[Any, ...]],
    ) -> None:
        super().__init__(flow_updates, log)
        self.correction_started = asyncio.Event()
        self.release_correction = asyncio.Event()

    async def request_command(
        self,
        command: str,
        arguments: Mapping[str, Any],
        *,
        timeout_seconds: float,
    ) -> Any:
        if command == "fold":
            self.correction_started.set()
            await self.release_correction.wait()
        return await super().request_command(
            command,
            arguments,
            timeout_seconds=timeout_seconds,
        )


class FlakyCommandsRobotSession(FakeRobotSession):
    def __init__(
        self,
        flow_updates: Sequence[FlowPointer | None],
        log: list[tuple[Any, ...]],
    ) -> None:
        super().__init__(flow_updates, log)
        self.command_waits = 0

    async def wait_for_commands(
        self,
        *,
        generation: int,
        timeout_seconds: float,
    ) -> CommandsFrame:
        self.command_waits += 1
        if self.command_waits == 1:
            raise RobotCommandNotSentError("temporary spec absence")
        return await super().wait_for_commands(
            generation=generation,
            timeout_seconds=timeout_seconds,
        )


class FakeRecoveryDatabase:
    def __init__(
        self,
        row: Mapping[str, Any],
        log: list[tuple[Any, ...]],
        *,
        max_attempts: int = 3,
        pinned_actions: Sequence[Mapping[str, Any]] | None = None,
        prepare_session_id_override: str | None = None,
    ) -> None:
        self.rows = {str(row["failure_id"]): copy.deepcopy(dict(row))}
        self.log = log
        self.max_attempts = max_attempts
        self.session: dict[str, Any] | None = None
        self.pinned_actions = (
            copy.deepcopy(list(pinned_actions))
            if pinned_actions is not None
            else None
        )
        self.prepare_session_id_override = prepare_session_id_override
        self.finish_results: list[str] = []
        self.non_executable: list[tuple[str, str]] = []
        self.expired: list[Mapping[str, Any]] = []
        self.candidates: list[str] = []

    async def fetch_failure(self, failure_id: str):
        self.log.append(("fetch", failure_id))
        row = self.rows.get(failure_id)
        return copy.deepcopy(row) if row is not None else None

    def _new_session(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        max_attempts: int,
        rewind_steps: int,
    ) -> dict[str, Any]:
        row = self.rows[failure_id]
        pinned = self.pinned_actions or row["resolver_suggestion"]["actions"]
        pinned_suggestion = suggestion(pinned)
        return {
            "recovery_session_id": recovery_session_id,
            "root_failure_id": failure_id,
            "current_failure_id": failure_id,
            "sysid": row["sysid"],
            "flow_id": row["flow_id"],
            "action_index": row["action_index"],
            "action_command": row["action_command"],
            "root_episode_key": row["recovery_episode_key"],
            "pinned_memory_id": MEMORY_ID,
            "pinned_resolver_suggestion": pinned_suggestion,
            "pinned_actions": copy.deepcopy(pinned),
            "pinned_actions_hash": "a" * 64,
            "recovery_status": "ready",
            "recovery_attempts": 0,
            "recovery_max_attempts": max_attempts,
            "recovery_rewind_steps": rewind_steps,
            "recovery_message": "ready",
            "recovery_run_token": None,
        }

    async def prepare(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        max_attempts: int,
        rewind_steps: int,
    ):
        self.log.append(("prepare", failure_id, rewind_steps))
        if self.session is None:
            self.session = self._new_session(
                failure_id=failure_id,
                recovery_session_id=(
                    self.prepare_session_id_override
                    or recovery_session_id
                ),
                max_attempts=max_attempts,
                rewind_steps=rewind_steps,
            )
        self.rows[failure_id]["recovery_session_id"] = self.session[
            "recovery_session_id"
        ]
        self.rows[failure_id]["recovery_status"] = self.session["recovery_status"]
        return copy.deepcopy(self.session)

    async def attach(self, *, failure_id: str, recovery_session_id: str):
        self.log.append(("attach", failure_id, recovery_session_id))
        assert self.session is not None
        self.session["current_failure_id"] = failure_id
        self.rows[failure_id]["recovery_session_id"] = recovery_session_id
        self.rows[failure_id]["recovery_status"] = self.session["recovery_status"]
        return copy.deepcopy(self.session)

    async def project(self, *, recovery_session_id: str):
        self.log.append(("project", recovery_session_id))
        assert self.session is not None
        return copy.deepcopy(self.session)

    async def claim(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        lease_seconds: int,
    ):
        self.log.append(("claim", failure_id))
        assert self.session is not None
        assert recovery_session_id == self.session["recovery_session_id"]
        self.session["current_failure_id"] = failure_id
        self.session["recovery_attempts"] += 1
        self.session["recovery_status"] = "running"
        self.session["recovery_run_token"] = RUN_TOKENS[
            self.session["recovery_attempts"] - 1
        ]
        return copy.deepcopy(self.session)

    async def finish(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        result: str,
        message: str | None,
    ):
        self.log.append(("finish", result))
        self.finish_results.append(result)
        assert self.session is not None
        if result == "failed" and (
            self.session["recovery_attempts"]
            >= self.session["recovery_max_attempts"]
        ):
            self.session["recovery_status"] = "timed_out"
        else:
            self.session["recovery_status"] = result
        return copy.deepcopy(self.session)

    async def expired_attempts(self, *, limit: int):
        return copy.deepcopy(self.expired[:limit])

    async def expire(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        message: str | None,
    ):
        self.log.append(("expire", recovery_session_id))
        row = copy.deepcopy(dict(self.expired[0]))
        row["recovery_status"] = "unknown"
        return row

    async def cold_candidates(self, *, sysids, limit: int):
        return tuple(self.candidates[:limit])

    async def mark_non_executable(
        self,
        *,
        failure_id: str,
        message: str,
    ) -> None:
        self.non_executable.append((failure_id, message))


class MalformedClaimDatabase(FakeRecoveryDatabase):
    async def claim(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        lease_seconds: int,
    ):
        claimed = await super().claim(
            failure_id=failure_id,
            recovery_session_id=recovery_session_id,
            lease_seconds=lease_seconds,
        )
        claimed["pinned_actions"] = [{"malformed": True}]
        return claimed


class MatcherRaceDatabase(FakeRecoveryDatabase):
    """Hold the pre-matcher read while the post-matcher signal coalesces."""

    def __init__(
        self,
        row: Mapping[str, Any],
        log: list[tuple[Any, ...]],
    ) -> None:
        super().__init__(row, log)
        self.first_fetch_seen = asyncio.Event()
        self.release_first_fetch = asyncio.Event()
        self.finished = asyncio.Event()
        self.fetch_count = 0

    async def fetch_failure(self, failure_id: str):
        self.fetch_count += 1
        if self.fetch_count == 1:
            self.log.append(("fetch", failure_id))
            snapshot = copy.deepcopy(self.rows[failure_id])
            self.first_fetch_seen.set()
            await self.release_first_fetch.wait()
            return snapshot
        return await super().fetch_failure(failure_id)

    async def finish(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        result: str,
        message: str | None,
    ):
        row = await super().finish(
            failure_id=failure_id,
            recovery_session_id=recovery_session_id,
            run_token=run_token,
            result=result,
            message=message,
        )
        if result != "awaiting_outcome":
            self.finished.set()
        return row


class FlowChangeOnPrepareDatabase(FakeRecoveryDatabase):
    def __init__(
        self,
        row: Mapping[str, Any],
        log: list[tuple[Any, ...]],
        session: FakeRobotSession,
    ) -> None:
        super().__init__(row, log)
        self.robot_session = session

    async def prepare(self, **kwargs):
        prepared = await super().prepare(**kwargs)
        self.robot_session.latest_flow = pointer(
            99,
            index=ACTION_INDEX + 1,
            command="close_drawer",
        )
        return prepared


class FlowChangeOnClaimDatabase(FakeRecoveryDatabase):
    def __init__(
        self,
        row: Mapping[str, Any],
        log: list[tuple[Any, ...]],
        session: FakeRobotSession,
        *,
        reconnect: bool = False,
    ) -> None:
        super().__init__(row, log)
        self.robot_session = session
        self.reconnect = reconnect

    async def claim(self, **kwargs):
        claimed = await super().claim(**kwargs)
        if self.reconnect:
            self.robot_session.generation += 1
        else:
            self.robot_session.latest_flow = pointer(
                99,
                index=ACTION_INDEX + 1,
                command="close_drawer",
            )
        return claimed


class FakeRpcResponse:
    def __init__(self, data: Any) -> None:
        self.data = data


class FakeRpcCall:
    def __init__(self, response: Mapping[str, Any]) -> None:
        self.response = response

    async def execute(self) -> FakeRpcResponse:
        return FakeRpcResponse([copy.deepcopy(dict(self.response))])


class FakeRpcClient:
    def __init__(self, response: Mapping[str, Any]) -> None:
        self.response = response
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    def rpc(self, name: str, parameters: Mapping[str, Any]) -> FakeRpcCall:
        self.calls.append((name, copy.deepcopy(dict(parameters))))
        return FakeRpcCall(self.response)


def execution_settings(
    *,
    max_attempts: int = 3,
    reconcile_interval_seconds: float = 5,
    start_grace_seconds: float = 0.0,
) -> RecoveryExecutionSettings:
    return RecoveryExecutionSettings(
        enabled=True,
        robot_allowlist=("BILLIE-16",),
        max_attempts=max_attempts,
        command_timeout_seconds=1,
        outcome_timeout_seconds=1,
        lease_seconds=30,
        reconcile_interval_seconds=reconcile_interval_seconds,
        cf_access_client_id="client-id",
        cf_access_client_secret="client-secret",
        start_grace_seconds=start_grace_seconds,
    )


def base_resolver_environment() -> dict[str, str]:
    return {
        "RESOLVER_MODE": "agent",
        "SUPABASE_URL": "https://example.supabase.co",
        "SUPABASE_SERVICE_ROLE_KEY": "service-role",
        "OPENAI_API_KEY": "openai",
        "MEMORY_REPO_URL": "https://example.test/memory.git",
    }


def test_auto_execution_is_off_by_default() -> None:
    settings = ResolverSettings.from_env(base_resolver_environment())

    assert settings.auto_execute is False
    assert settings.recovery_robot_allowlist == ()
    assert settings.recovery_max_attempts == 3
    assert settings.recovery_lease_seconds == 300


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {"RESOLVER_AUTO_EXECUTE": "true"},
            "RECOVERY_ROBOT_ALLOWLIST",
        ),
        (
            {
                "RESOLVER_AUTO_EXECUTE": "true",
                "RECOVERY_ROBOT_ALLOWLIST": "BILLIE-16",
            },
            "RECOVERY_CF_ACCESS_CLIENT_ID",
        ),
        (
            {"RESOLVER_AUTO_EXECUTE": "maybe"},
            "RESOLVER_AUTO_EXECUTE",
        ),
        (
            {
                "RESOLVER_AUTO_EXECUTE": "true",
                "RECOVERY_ROBOT_ALLOWLIST": "BILLIE-16",
                "RECOVERY_COMMAND_TIMEOUT_SECONDS": "15",
                "RECOVERY_OUTCOME_TIMEOUT_SECONDS": "60",
                "RECOVERY_LEASE_SECONDS": "100",
                "RECOVERY_CF_ACCESS_CLIENT_ID": "client-id",
                "RECOVERY_CF_ACCESS_CLIENT_SECRET": "client-secret",
            },
            "RECOVERY_LEASE_SECONDS",
        ),
    ],
)
def test_auto_execution_requires_explicit_safe_settings(
    updates: Mapping[str, str],
    message: str,
) -> None:
    environment = base_resolver_environment()
    environment.update(updates)

    with pytest.raises(ValueError, match=message):
        ResolverSettings.from_env(environment)


def test_auto_execution_settings_are_normalized_and_bounded() -> None:
    environment = base_resolver_environment()
    environment.update(
        {
            "RESOLVER_AUTO_EXECUTE": "true",
            "RECOVERY_ROBOT_ALLOWLIST": "billie-16, BILLIE-16,billie-17",
            "RECOVERY_MAX_ATTEMPTS": "4",
            "RECOVERY_COMMAND_TIMEOUT_SECONDS": "8",
            "RECOVERY_OUTCOME_TIMEOUT_SECONDS": "45",
            "RECOVERY_LEASE_SECONDS": "130",
            "RECOVERY_CF_ACCESS_CLIENT_ID": "client-id",
            "RECOVERY_CF_ACCESS_CLIENT_SECRET": "client-secret",
        }
    )

    settings = ResolverSettings.from_env(environment)

    assert settings.auto_execute is True
    assert settings.recovery_robot_allowlist == ("BILLIE-16", "BILLIE-17")
    assert settings.recovery_max_attempts == 4
    assert settings.recovery_command_timeout_seconds == 8
    assert settings.recovery_outcome_timeout_seconds == 45
    assert settings.recovery_lease_seconds == 130


def test_parser_maps_only_terminal_legacy_rerun_to_guarded_retry() -> None:
    plan = parse_recovery_plan([correction_action(), rerun_action()])

    assert [action.command for action in plan.corrections] == ["fold"]
    assert plan.corrections[0].arguments_effective == {
        "wait": True,
        "speed": 60,
    }
    assert plan.corrections[0].explicit_arguments == ("wait", "speed")
    assert plan.rewind_steps == 0
    assert plan.legacy_retry_index == ACTION_INDEX
    assert plan.legacy_retry_command == ACTION_COMMAND


def test_parser_accepts_guarded_rewind_up_to_five_steps() -> None:
    plan = parse_recovery_plan([correction_action(), resume_action(5)])

    assert plan.rewind_steps == 5
    assert plan.resume_mode == "rewind"


@pytest.mark.parametrize(
    "actions",
    [
        [correction_action()],
        [rerun_action(), correction_action()],
        [
            {
                **correction_action(),
                "unexpected": "not executable",
            },
            rerun_action(),
        ],
        [correction_action(), resume_action(6)],
        [
            {
                "command": "$abort",
                "title": "Abort",
                "arguments": {},
            },
            rerun_action(),
        ],
    ],
)
def test_parser_rejects_ambiguous_or_unsafe_sequences(
    actions: Sequence[Mapping[str, Any]],
) -> None:
    with pytest.raises(RecoveryContractError):
        parse_recovery_plan(actions)


def test_parser_requires_complete_consistent_modern_continuation_context(
) -> None:
    invalid_actions: list[dict[str, Any]] = []

    missing_flow_id = resume_action(0)
    missing_flow_id["arguments"].pop("flow_id")
    missing_flow_id["explicit_arguments"].remove("flow_id")
    invalid_actions.append(missing_flow_id)

    rewind_zero = resume_action(1)
    rewind_zero["arguments"]["rewind_steps"] = 0
    rewind_zero["arguments_effective"]["rewind_steps"] = 0
    rewind_zero["continuation_context"]["expected_arguments"][
        "rewind_steps"
    ] = 0
    invalid_actions.append(rewind_zero)

    missing_context = resume_action(0)
    missing_context.pop("continuation_context")
    invalid_actions.append(missing_context)

    current_mismatch = resume_action(0)
    current_mismatch["continuation_context"]["current_action"][
        "command"
    ] = "close_drawer"
    invalid_actions.append(current_mismatch)

    target_mismatch = resume_action(2)
    target_mismatch["continuation_context"]["target_action"][
        "actionIndex"
    ] = ACTION_INDEX - 1
    invalid_actions.append(target_mismatch)

    retry_target_mismatch = resume_action(0)
    retry_target_mismatch["continuation_context"]["target_action"][
        "areaName"
    ] = "Entry"
    invalid_actions.append(retry_target_mismatch)

    for actions in invalid_actions:
        with pytest.raises(RecoveryContractError):
            parse_recovery_plan([correction_action(), actions])


def test_live_action_spec_validation_is_strict_and_does_not_default() -> None:
    frame = CommandsFrame(
        revision=9,
        generation=4,
        commands={
            "configure": {
                "name": "configure",
                "inputs": [
                    {
                        "name": "mode",
                        "type": "enum",
                        "enum": ["safe", "fast"],
                        "required": True,
                    },
                    {
                        "name": "options.retries",
                        "type": "int",
                        "required": True,
                        "minimum": 1,
                        "maximum": 3,
                    },
                    {
                        "name": "enabled",
                        "type": "boolean",
                        "required": False,
                    },
                ],
            }
        },
    )
    valid = RecoveryAction(
        command="configure",
        arguments={
            "mode": "safe",
            "options": {"retries": 2},
            "enabled": True,
        },
    )

    validate_corrections_against_commands(
        [valid],
        frame,
        generation=4,
    )

    invalid_arguments = (
        {"mode": "unsafe", "options": {"retries": 2}},
        {"mode": "safe", "options": {"retries": 0}},
        {"mode": "safe", "options": {"retries": 2.5}},
        {"mode": "safe"},
        {
            "mode": "safe",
            "options": {"retries": 2},
            "invented": True,
        },
    )
    for arguments in invalid_arguments:
        with pytest.raises(RecoveryContractError):
            validate_corrections_against_commands(
                [RecoveryAction("configure", arguments)],
                frame,
                generation=4,
            )
    with pytest.raises(RecoveryContractError, match="old robot connection"):
        validate_corrections_against_commands(
            [valid],
            frame,
            generation=5,
        )


def test_live_action_defaults_require_matching_recorded_provenance() -> None:
    frame = CommandsFrame(
        revision=1,
        generation=1,
        commands={
            "fold": {
                "name": "fold",
                "inputs": [
                    {
                        "name": "wait",
                        "type": "boolean",
                        "required": False,
                        "placeholder": "false",
                    },
                    {
                        "name": "speed",
                        "type": "float",
                        "required": False,
                        "placeholder": "60",
                    },
                ],
            }
        },
    )

    with pytest.raises(RecoveryContractError, match="did not record"):
        validate_corrections_against_commands(
            [RecoveryAction("fold", {"wait": True})],
            frame,
            generation=1,
        )

    with pytest.raises(RecoveryContractError, match="defaults changed"):
        validate_corrections_against_commands(
            [
                RecoveryAction(
                    "fold",
                    {"wait": True},
                    arguments_effective={"wait": True, "speed": 40},
                    explicit_arguments=("wait",),
                )
            ],
            frame,
            generation=1,
        )

    with pytest.raises(RecoveryContractError, match="explicit arguments"):
        validate_corrections_against_commands(
            [
                RecoveryAction(
                    "fold",
                    {"wait": True},
                    arguments_effective={"wait": True, "speed": 60},
                    explicit_arguments=("speed",),
                )
            ],
            frame,
            generation=1,
        )

    validate_corrections_against_commands(
        [
            RecoveryAction(
                "fold",
                {"wait": True},
                arguments_effective={"wait": True, "speed": 60},
                explicit_arguments=("wait",),
            )
        ],
        frame,
        generation=1,
    )


def test_confirmation_commands_cannot_be_made_auto_safe_by_live_spec() -> None:
    action = RecoveryAction(
        "download_vla_model",
        {"model_id": "bellboy/model"},
    )
    unsafe = CommandsFrame(
        revision=1,
        generation=1,
        commands={
            "download_vla_model": {
                "name": "download_vla_model",
                "inputs": [
                    {
                        "name": "model_id",
                        "type": "string",
                        "required": True,
                    }
                ],
            }
        },
    )
    with pytest.raises(RecoveryContractError, match="confirmation"):
        validate_corrections_against_commands(
            [action],
            unsafe,
            generation=1,
        )

    safe_schema = copy.deepcopy(dict(unsafe.commands))
    safe_schema["download_vla_model"]["auto_safe"] = True
    with pytest.raises(RecoveryContractError, match="confirmation"):
        validate_corrections_against_commands(
            [action],
            CommandsFrame(2, 1, safe_schema),
            generation=1,
        )

    live_confirmed = {
        "fold": {
            "name": "fold",
            "inputs": [],
            "requires_confirmation": True,
            "auto_safe": True,
        }
    }
    with pytest.raises(RecoveryContractError, match="confirmation"):
        validate_corrections_against_commands(
            [RecoveryAction("fold", {})],
            CommandsFrame(3, 1, live_confirmed),
            generation=1,
        )


def test_completed_flow_pointer_is_recovered_without_fake_action() -> None:
    completed = FlowPointer(
        revision=20,
        flow_id=FLOW_ID,
        status="ready",
        action_index=None,
        action_command=None,
        filename=None,
        fid=None,
        flow_commit=None,
        raw={"id": FLOW_ID, "status": "ready"},
        generation=1,
        completed=True,
    )
    database = FakeRecoveryDatabase(failure_row(), [])
    record = parse_recovery_session(
        database._new_session(
            failure_id=FAILURE_ID,
            recovery_session_id=SESSION_ID,
            max_attempts=3,
            rewind_steps=0,
        )
    )

    assert classify_flow_outcome(completed, record) == "recovered"


@pytest.mark.anyio
async def test_duplicate_notification_replays_after_matcher_pins_solution(
) -> None:
    log: list[tuple[Any, ...]] = []
    row = failure_row()
    row["matcher_status"] = "pending"
    row["resolver_suggestion"] = None
    database = MatcherRaceDatabase(row, log)
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )
    await coordinator.start()

    coordinator.notify_failure(FAILURE_ID)
    await asyncio.wait_for(database.first_fetch_seen.wait(), timeout=1)
    database.rows[FAILURE_ID].update(failure_row())
    coordinator.notify_failure(FAILURE_ID)
    database.release_first_fetch.set()

    await asyncio.wait_for(database.finished.wait(), timeout=1)
    await coordinator.stop()

    assert database.fetch_count >= 3
    assert database.finish_results == ["awaiting_outcome", "recovered"]
    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]
    assert coordinator.state.events_enqueued == 2


@pytest.mark.anyio
async def test_coordinator_prepares_claims_and_runs_exact_order_to_advancement(
) -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    labels = [entry[0] for entry in log]
    assert labels.index("prepare") < labels.index("claim") < labels.index(
        "command"
    )
    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]
    assert session.commands[0][1] == {"wait": True, "speed": 60}
    assert session.commands[1][1] == {
        "mode": "retry_current",
        "rewind_steps": 0,
        "current_action_index": ACTION_INDEX,
        "current_command": ACTION_COMMAND,
        "flow_id": FLOW_ID,
    }
    assert database.finish_results == ["awaiting_outcome", "recovered"]
    assert coordinator.state.attempts_claimed == 1
    assert coordinator.state.attempts_recovered == 1
    assert coordinator.state.actions_accepted == 2


@pytest.mark.anyio
async def test_operator_cancel_during_start_grace_prevents_claim() -> None:
    log: list[tuple[Any, ...]] = []

    class CancellingDatabase(FakeRecoveryDatabase):
        async def project(self, *, recovery_session_id: str):
            assert self.session is not None
            self.session["recovery_status"] = "cancelled"
            return await super().project(
                recovery_session_id=recovery_session_id
            )

    database = CancellingDatabase(failure_row(), log)
    session = FakeRobotSession([pointer(2)], log)
    coordinator = RecoveryCoordinator(
        execution_settings(start_grace_seconds=0.01),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    labels = [entry[0] for entry in log]
    assert "claim" not in labels
    assert coordinator.state.attempts_claimed == 0
    assert session.commands == []


@pytest.mark.anyio
async def test_current_generation_pause_does_not_require_duplicate_publication(
) -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    session = FakeRobotSession(
        [
            pointer(
                2,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            )
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]
    assert database.finish_results == ["awaiting_outcome", "recovered"]
    assert ("wait_flow", 1) in log


@pytest.mark.anyio
async def test_modern_terminal_resume_is_sent_once_with_exact_pinned_arguments(
) -> None:
    log: list[tuple[Any, ...]] = []
    terminal = resume_action(2)
    database = FakeRecoveryDatabase(
        failure_row(actions=(correction_action(), terminal)),
        log,
    )
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]
    assert session.commands[-1][1] == terminal["arguments"]
    assert database.session is not None
    assert database.session["recovery_rewind_steps"] == 2


@pytest.mark.anyio
async def test_resume_ignores_jump_pause_until_execution_then_recurrence(
) -> None:
    log: list[tuple[Any, ...]] = []
    terminal = resume_action(2)
    database = FakeRecoveryDatabase(
        failure_row(actions=(correction_action(), terminal)),
        log,
        max_attempts=1,
    )
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                index=ACTION_INDEX - 2,
                command="previous",
            ),
            pointer(
                4,
                status="in_progress",
                index=ACTION_INDEX - 2,
                command="previous",
            ),
            pointer(5),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(max_attempts=1),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert database.finish_results == ["awaiting_outcome", "failed"]
    assert database.session is not None
    assert database.session["recovery_status"] == "timed_out"
    assert coordinator.state.sessions_timed_out == 1


@pytest.mark.anyio
async def test_unknown_live_correction_becomes_visible_manual_fallback(
) -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(
        failure_row(
            actions=(correction_action("unknown_command"), rerun_action())
        ),
        log,
    )
    session = FakeRobotSession([pointer(2)], log)
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert session.commands == []
    assert all(entry[0] not in ("prepare", "claim") for entry in log)
    assert len(database.non_executable) == 1
    assert "manual handling" in database.non_executable[0][1]


@pytest.mark.parametrize("terminal", [rerun_action(), resume_action(0)])
@pytest.mark.anyio
async def test_reused_index_and_command_with_changed_flow_structure_is_blocked(
    terminal: Mapping[str, Any],
) -> None:
    log: list[tuple[Any, ...]] = []
    changed = pointer(2)
    changed.raw["areas"][ACTION_INDEX]["name"] = "Different area"
    database = FakeRecoveryDatabase(
        failure_row(actions=(correction_action(), terminal)),
        log,
    )
    session = FakeRobotSession([], log)
    session.latest_flow = changed
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert all(entry[0] not in ("prepare", "claim") for entry in log)
    assert len(database.non_executable) == 1


@pytest.mark.anyio
async def test_legacy_retry_is_blocked_when_expected_next_action_changed(
) -> None:
    log: list[tuple[Any, ...]] = []
    changed = pointer(2)
    next_action = changed.raw["areas"][ACTION_INDEX + 1]["items"][0][
        "actions"
    ][0]
    next_action["command"] = "different_next_action"
    database = FakeRecoveryDatabase(failure_row(), log)
    session = FakeRobotSession([], log)
    session.latest_flow = changed
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert all(entry[0] not in ("prepare", "claim") for entry in log)
    assert len(database.non_executable) == 1


@pytest.mark.anyio
async def test_flow_change_during_prepare_does_not_consume_or_dispatch(
) -> None:
    log: list[tuple[Any, ...]] = []
    session = FakeRobotSession([pointer(2)], log)
    database = FlowChangeOnPrepareDatabase(
        failure_row(),
        log,
        session,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert session.commands == []
    assert all(entry[0] != "claim" for entry in log)
    assert database.non_executable == []


@pytest.mark.parametrize("reconnect", [False, True])
@pytest.mark.anyio
async def test_flow_or_generation_change_after_claim_finishes_unknown(
    reconnect: bool,
) -> None:
    log: list[tuple[Any, ...]] = []
    session = FakeRobotSession([pointer(2)], log)
    database = FlowChangeOnClaimDatabase(
        failure_row(),
        log,
        session,
        reconnect=reconnect,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert session.commands == []
    assert database.finish_results == ["unknown"]
    assert coordinator.state.attempts_unknown == 1


@pytest.mark.anyio
async def test_same_fresh_failure_retries_pinned_plan_until_timed_out() -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log, max_attempts=3)
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(3, status="in_progress"),
            pointer(4),
            pointer(5, status="in_progress"),
            pointer(6),
            pointer(7, status="in_progress"),
            pointer(8),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(max_attempts=3),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
        "fold",
        "$resume_flow",
        "fold",
        "$resume_flow",
    ]
    assert database.finish_results == [
        "awaiting_outcome",
        "failed",
        "awaiting_outcome",
        "failed",
        "awaiting_outcome",
        "failed",
    ]
    assert database.session is not None
    assert database.session["recovery_status"] == "timed_out"
    assert coordinator.state.attempts_claimed == 3
    assert coordinator.state.attempts_failed == 3
    assert coordinator.state.sessions_timed_out == 1


@pytest.mark.anyio
async def test_ambiguous_command_outcome_is_unknown_and_never_retried() -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    session = FakeRobotSession(
        [pointer(2)],
        log,
        command_error=RobotCommandOutcomeUnknownError("connection closed"),
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert [command for command, _arguments in session.commands] == ["fold"]
    assert database.finish_results == ["unknown"]
    assert database.session is not None
    assert database.session["recovery_status"] == "unknown"
    assert coordinator.state.attempts_unknown == 1


@pytest.mark.anyio
async def test_malformed_claim_is_closed_as_unknown_before_any_dispatch() -> None:
    log: list[tuple[Any, ...]] = []
    database = MalformedClaimDatabase(failure_row(), log)
    session = FakeRobotSession([pointer(2)], log)
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert session.commands == []
    assert database.finish_results == ["unknown"]
    assert coordinator.state.attempts_claimed == 1
    assert coordinator.state.attempts_unknown == 1


def test_enabled_settings_reject_lease_shorter_than_worst_case_plan() -> None:
    with pytest.raises(ValueError, match="RECOVERY_LEASE_SECONDS"):
        RecoveryExecutionSettings(
            enabled=True,
            robot_allowlist=("BILLIE-16",),
            max_attempts=3,
            command_timeout_seconds=1,
            outcome_timeout_seconds=1,
            lease_seconds=5,
            reconcile_interval_seconds=1,
            cf_access_client_id="client-id",
            cf_access_client_secret="client-secret",
        )


@pytest.mark.anyio
async def test_existing_session_executes_claimed_pinned_actions_not_row_drift(
) -> None:
    log: list[tuple[Any, ...]] = []
    row = failure_row(
        actions=(correction_action("bump"), rerun_action()),
        recovery_session_id=SESSION_ID,
        recovery_status="ready",
    )
    database = FakeRecoveryDatabase(
        row,
        log,
        pinned_actions=(correction_action("fold"), rerun_action()),
    )
    database.session = database._new_session(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        max_attempts=3,
        rewind_steps=0,
    )
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]
    assert ("project", SESSION_ID) in log
    assert all(entry[0] != "prepare" for entry in log)


@pytest.mark.anyio
async def test_prepare_adopts_database_reused_session_id_across_restart() -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(
        failure_row(),
        log,
        prepare_session_id_override=SESSION_ID,
    )
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert coordinator.state.last_session_id == SESSION_ID
    assert database.session is not None
    assert database.session["recovery_session_id"] == SESSION_ID
    assert database.session["recovery_status"] == "recovered"


@pytest.mark.anyio
async def test_new_failure_recurrence_attaches_to_in_flight_session() -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    database.rows[SECOND_FAILURE_ID] = failure_row(
        failure_id=SECOND_FAILURE_ID
    )
    database.rows[SECOND_FAILURE_ID].update(
        {
            "analysis_status": "pending",
            "matcher_status": "pending",
            "resolver_suggestion": None,
        }
    )
    session = BlockingRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    first = asyncio.create_task(coordinator.process_failure(FAILURE_ID))
    await session.correction_started.wait()
    await coordinator.process_failure(SECOND_FAILURE_ID)
    session.release_correction.set()
    await first

    assert database.session is not None
    assert database.session["current_failure_id"] == SECOND_FAILURE_ID
    assert coordinator.state.recurrences_attached == 1
    assert any(
        entry[0] == "attach" and entry[1] == SECOND_FAILURE_ID
        for entry in log
    )


@pytest.mark.anyio
async def test_confirmed_failed_session_continues_remaining_budget_after_restart(
) -> None:
    log: list[tuple[Any, ...]] = []
    row = failure_row(
        recovery_session_id=SESSION_ID,
        recovery_status="failed",
    )
    database = FakeRecoveryDatabase(row, log)
    database.session = database._new_session(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        max_attempts=3,
        rewind_steps=0,
    )
    database.session.update(
        {
            "recovery_status": "failed",
            "recovery_attempts": 1,
            "recovery_run_token": RUN_TOKENS[0],
        }
    )
    session = FakeRobotSession(
        [
            pointer(2),
            pointer(
                3,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert database.session["recovery_attempts"] == 2
    assert database.session["recovery_status"] == "recovered"
    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]


@pytest.mark.anyio
async def test_active_or_ambiguous_session_is_never_resent() -> None:
    log: list[tuple[Any, ...]] = []
    row = failure_row(
        recovery_session_id=SESSION_ID,
        recovery_status="awaiting_outcome",
    )
    database = FakeRecoveryDatabase(row, log)
    session = FakeRobotSession([], log)
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )

    await coordinator.process_failure(FAILURE_ID)

    assert session.commands == []
    assert all(entry[0] not in ("project", "claim", "prepare") for entry in log)
    assert coordinator.state.events_skipped == 1


@pytest.mark.anyio
async def test_cold_reconcile_expires_ambiguous_attempt_and_only_enqueues_safe(
) -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    database.session = database._new_session(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        max_attempts=3,
        rewind_steps=0,
    )
    database.session.update(
        {
            "recovery_status": "awaiting_outcome",
            "recovery_attempts": 1,
            "recovery_run_token": RUN_TOKENS[0],
        }
    )
    database.expired = [copy.deepcopy(database.session)]
    database.candidates = [FAILURE_ID]
    session = FakeRobotSession([], log)
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=database,
        sessions={"BILLIE-16": session},
    )
    coordinator._started = True

    await coordinator.reconcile()
    await coordinator.stop()

    assert ("expire", SESSION_ID) in log
    assert coordinator.state.expired_attempts_reconciled == 1
    assert coordinator.state.attempts_unknown == 1
    # Reconciliation never enqueues running/awaiting rows itself; the database
    # adapter supplies only null/ready/confirmed-failed candidates.
    assert coordinator.state.events_enqueued == 1


@pytest.mark.anyio
async def test_periodic_reconcile_expires_lease_that_was_initially_live(
) -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    database.session = database._new_session(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        max_attempts=3,
        rewind_steps=0,
    )
    database.session.update(
        {
            "recovery_status": "awaiting_outcome",
            "recovery_attempts": 1,
            "recovery_run_token": RUN_TOKENS[0],
        }
    )
    coordinator = RecoveryCoordinator(
        execution_settings(reconcile_interval_seconds=0.01),
        database=database,
        sessions={"BILLIE-16": FakeRobotSession([], log)},
    )
    await coordinator.start()
    await asyncio.sleep(0.015)
    database.expired = [copy.deepcopy(database.session)]

    for _ in range(50):
        if ("expire", SESSION_ID) in log:
            break
        await asyncio.sleep(0.01)
    await coordinator.stop()

    assert ("expire", SESSION_ID) in log
    assert coordinator.state.expired_attempts_reconciled == 1


@pytest.mark.anyio
async def test_periodic_reconcile_retries_transient_preclaim_spec_absence(
) -> None:
    log: list[tuple[Any, ...]] = []
    database = FakeRecoveryDatabase(failure_row(), log)
    database.candidates = [FAILURE_ID]
    session = FlakyCommandsRobotSession(
        [
            pointer(2),
            pointer(3),
            pointer(
                4,
                status="in_progress",
                index=ACTION_INDEX + 1,
                command="close_drawer",
            ),
        ],
        log,
    )
    coordinator = RecoveryCoordinator(
        execution_settings(reconcile_interval_seconds=0.01),
        database=database,
        sessions={"BILLIE-16": session},
    )
    await coordinator.start()

    for _ in range(100):
        if "recovered" in database.finish_results:
            break
        await asyncio.sleep(0.01)
    await coordinator.stop()

    assert session.command_waits >= 2
    assert database.finish_results[-1] == "recovered"
    assert [command for command, _arguments in session.commands] == [
        "fold",
        "$resume_flow",
    ]


def test_counter_snapshot_exposes_auto_recovery_lifecycle() -> None:
    state = RecoveryExecutionState(
        attempts_claimed=3,
        attempts_failed=3,
        sessions_timed_out=1,
    )

    snapshot = state.snapshot(enabled=True)

    assert snapshot["auto_recovery_enabled"] is True
    assert snapshot["auto_recovery_attempts_claimed"] == 3
    assert snapshot["auto_recovery_attempts_failed"] == 3
    assert snapshot["auto_recovery_sessions_timed_out"] == 1


def test_coordinator_snapshot_exposes_robot_ws_and_spec_readiness() -> None:
    session = FakeRobotSession([], [])
    coordinator = RecoveryCoordinator(
        execution_settings(),
        database=FakeRecoveryDatabase(failure_row(), []),
        sessions={"BILLIE-16": session},
    )

    snapshot = coordinator.snapshot()

    assert snapshot["auto_recovery_robots"]["BILLIE-16"] == {
        "connected": True,
        "generation": 1,
        "flow_ready": True,
        "commands_ready": True,
    }


def test_guarded_resume_rebuilds_identity_from_live_pointer() -> None:
    live = pointer(42)

    arguments = build_guarded_resume_arguments(live, rewind_steps=2)

    assert arguments == {
        "mode": "rewind",
        "rewind_steps": 2,
        "current_action_index": ACTION_INDEX,
        "current_command": ACTION_COMMAND,
        "flow_id": FLOW_ID,
    }


@pytest.mark.anyio
async def test_supabase_adapter_uses_settled_token_bound_rpc_contract() -> None:
    response = {"recovery_session_id": SESSION_ID}
    client = FakeRpcClient(response)
    database = SupabaseRecoveryDatabase(client)

    assert await database.prepare(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        max_attempts=3,
        rewind_steps=2,
    ) == response
    assert await database.claim(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        lease_seconds=300,
    ) == response
    assert await database.finish(
        failure_id=FAILURE_ID,
        recovery_session_id=SESSION_ID,
        run_token=RUN_TOKENS[0],
        result="awaiting_outcome",
        message="waiting",
    ) == response

    assert client.calls == [
        (
            "prepare_failure_recovery",
            {
                "p_failure_id": FAILURE_ID,
                "p_recovery_session_id": SESSION_ID,
                "p_max_attempts": 3,
                "p_rewind_steps": 2,
            },
        ),
        (
            "claim_failure_recovery_attempt",
            {
                "p_failure_id": FAILURE_ID,
                "p_recovery_session_id": SESSION_ID,
                "p_lease_seconds": 300,
            },
        ),
        (
            "finish_failure_recovery_attempt",
            {
                "p_failure_id": FAILURE_ID,
                "p_recovery_session_id": SESSION_ID,
                "p_run_token": RUN_TOKENS[0],
                "p_result": "awaiting_outcome",
                "p_message": "waiting",
            },
        ),
    ]
