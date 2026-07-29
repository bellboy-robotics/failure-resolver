"""Bounded, service-owned automatic recovery execution.

The database owns retry accounting and pins one immutable action plan.  This
module owns robot command correlation and observes fresh Flow state to decide
whether that plan recovered the failed step.  It deliberately fails closed:
an ambiguous command or Flow outcome becomes terminal ``unknown`` and is never
resent automatically.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from robot_session import (
    CommandReceipt,
    CommandsFrame,
    FlowPointer,
    RobotCommandNotSentError,
    RobotCommandOutcomeUnknownError,
    RobotCommandRejectedError,
    RobotSession,
    parse_flow_pointer,
)


logger = logging.getLogger("failure_resolver.recovery")

_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_EPISODE_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SYSID_PATTERN = re.compile(r"^[A-Za-z0-9-]{1,64}$")
_TERMINAL_COMMANDS = frozenset(("$rerun", "$resume_flow"))
_SESSION_STATUSES = frozenset(
    (
        "ready",
        "running",
        "awaiting_outcome",
        "recovered",
        "failed",
        "unknown",
        "timed_out",
        "cancelled",
    )
)
_TERMINAL_SESSION_STATUSES = frozenset(
    ("recovered", "unknown", "timed_out", "cancelled")
)
_SUCCESS_FLOW_STATUSES = frozenset(
    ("ready", "completed", "complete", "finished", "succeeded", "success")
)
_ALLOWED_ACTION_FIELDS = frozenset(
    (
        "command",
        "title",
        "arguments",
        "arguments_effective",
        "explicit_arguments",
        "retry_context",
        "continuation_context",
    )
)
_ALLOWED_RESUME_ARGUMENTS = frozenset(
    (
        "mode",
        "rewind_steps",
        "current_action_index",
        "current_command",
        "flow_id",
        "filename",
        "fid",
        "flow_commit",
    )
)
_MAX_ACTIONS = 10
_MAX_REWIND_STEPS = 5
_MAX_COMMAND_LENGTH = 128
_MAX_ARGUMENT_BYTES = 32 * 1024
_MAX_JSON_DEPTH = 12
_MAX_JSON_NODES = 2_000
_MAX_PENDING_FAILURES = 512
_CONFIRMED_COMMANDS = frozenset(("download_vla_model",))
_CONDITIONAL_CONFIRMATION_COMMAND = "override_vla_model"
_MISSING = object()


class RecoveryContractError(ValueError):
    """Untrusted recovery data did not satisfy the executable contract."""


class RecoveryNonExecutableError(RecoveryContractError):
    """A pinned recovery is permanently unsafe against current contracts."""


class RecoveryLiveStateChanged(RecoveryContractError):
    """The robot moved or reconnected before a safe dispatch boundary."""


class RecoveryDatabaseUnavailable(ConnectionError):
    """The Supabase control plane is not currently available."""


@dataclass(frozen=True)
class RecoveryExecutionSettings:
    enabled: bool = False
    robot_allowlist: tuple[str, ...] = ()
    max_attempts: int = 3
    command_timeout_seconds: float = 15.0
    outcome_timeout_seconds: float = 60.0
    lease_seconds: int = 300
    cf_access_client_id: str = field(default="", repr=False)
    cf_access_client_secret: str = field(default="", repr=False)
    reconcile_limit: int = 500
    reconcile_interval_seconds: float = 30.0
    start_grace_seconds: float = 5.0

    def __post_init__(self) -> None:
        normalized: list[str] = []
        for sysid in self.robot_allowlist:
            canonical = sysid.strip().upper()
            if not _SYSID_PATTERN.fullmatch(canonical):
                raise ValueError(
                    "RECOVERY_ROBOT_ALLOWLIST contains an invalid robot sysid"
                )
            if canonical not in normalized:
                normalized.append(canonical)
        object.__setattr__(self, "robot_allowlist", tuple(normalized))

        if not 1 <= self.max_attempts <= 20:
            raise ValueError("RECOVERY_MAX_ATTEMPTS must be between 1 and 20")
        if self.command_timeout_seconds <= 0:
            raise ValueError(
                "RECOVERY_COMMAND_TIMEOUT_SECONDS must be greater than zero"
            )
        if self.outcome_timeout_seconds <= 0:
            raise ValueError(
                "RECOVERY_OUTCOME_TIMEOUT_SECONDS must be greater than zero"
            )
        if not 5 <= self.lease_seconds <= 900:
            raise ValueError("RECOVERY_LEASE_SECONDS must be between 5 and 900")
        if self.reconcile_limit <= 0:
            raise ValueError("reconcile_limit must be greater than zero")
        if self.start_grace_seconds < 0:
            raise ValueError(
                "RECOVERY_START_GRACE_SECONDS must be zero or greater"
            )
        if (
            self.reconcile_interval_seconds <= 0
            or self.reconcile_interval_seconds >= self.lease_seconds
        ):
            raise ValueError(
                "RECOVERY_RECONCILE_INTERVAL_SECONDS must be greater than "
                "zero and shorter than RECOVERY_LEASE_SECONDS"
            )
        if self.enabled:
            if not self.robot_allowlist:
                raise ValueError(
                    "RECOVERY_ROBOT_ALLOWLIST is required when "
                    "RESOLVER_AUTO_EXECUTE=true"
                )
            if (
                not self.cf_access_client_id.strip()
                or not self.cf_access_client_secret.strip()
            ):
                raise ValueError(
                    "RECOVERY_CF_ACCESS_CLIENT_ID and "
                    "RECOVERY_CF_ACCESS_CLIENT_SECRET are required when "
                    "RESOLVER_AUTO_EXECUTE=true"
                )
            required_lease_seconds = math.ceil(
                10 * self.command_timeout_seconds
                + self.outcome_timeout_seconds
                + 5.0
            )
            if self.lease_seconds < required_lease_seconds:
                raise ValueError(
                    "RECOVERY_LEASE_SECONDS must cover the worst-case "
                    "10-command plus Flow-outcome timeout window and safety "
                    "margin when RESOLVER_AUTO_EXECUTE=true"
                )


@dataclass
class RecoveryExecutionState:
    events_enqueued: int = 0
    events_skipped: int = 0
    sessions_prepared: int = 0
    recurrences_attached: int = 0
    attempts_claimed: int = 0
    actions_accepted: int = 0
    attempts_recovered: int = 0
    attempts_failed: int = 0
    attempts_unknown: int = 0
    sessions_timed_out: int = 0
    expired_attempts_reconciled: int = 0
    execution_errors: int = 0
    last_failure_id: str | None = None
    last_session_id: str | None = None
    last_error_type: str | None = None

    def snapshot(self, *, enabled: bool) -> dict[str, Any]:
        return {
            "auto_recovery_enabled": enabled,
            "auto_recovery_events_enqueued": self.events_enqueued,
            "auto_recovery_events_skipped": self.events_skipped,
            "auto_recovery_sessions_prepared": self.sessions_prepared,
            "auto_recovery_recurrences_attached": self.recurrences_attached,
            "auto_recovery_attempts_claimed": self.attempts_claimed,
            "auto_recovery_actions_accepted": self.actions_accepted,
            "auto_recovery_attempts_recovered": self.attempts_recovered,
            "auto_recovery_attempts_failed": self.attempts_failed,
            "auto_recovery_attempts_unknown": self.attempts_unknown,
            "auto_recovery_sessions_timed_out": self.sessions_timed_out,
            "auto_recovery_expired_attempts_reconciled": (
                self.expired_attempts_reconciled
            ),
            "auto_recovery_execution_errors": self.execution_errors,
            "auto_recovery_last_failure_id": self.last_failure_id,
            "auto_recovery_last_session_id": self.last_session_id,
            "auto_recovery_last_error_type": self.last_error_type,
        }


@dataclass(frozen=True)
class RecoveryAction:
    command: str
    arguments: Mapping[str, Any]
    arguments_effective: Mapping[str, Any] | None = None
    explicit_arguments: tuple[str, ...] | None = None


@dataclass(frozen=True)
class FlowActionContext:
    action_index: int
    command: str
    area_name: str
    item_name: str


@dataclass(frozen=True)
class RecoveryPlan:
    corrections: tuple[RecoveryAction, ...]
    terminal_command: str
    terminal_arguments: Mapping[str, Any]
    rewind_steps: int
    resume_mode: str
    legacy_retry_index: int | None = None
    legacy_retry_command: str | None = None
    legacy_retry_context: FlowActionContext | None = None
    legacy_expected_next: FlowActionContext | None = None
    legacy_expected_next_present: bool = False
    captured_action_index: int | None = None
    captured_action_command: str | None = None
    captured_current_context: FlowActionContext | None = None
    captured_target_context: FlowActionContext | None = None


@dataclass(frozen=True)
class FailurePointerTarget:
    failure_id: str
    sysid: str
    flow_id: str
    action_index: int
    action_command: str
    recovery_episode_key: str
    recovery_session_id: str | None
    recovery_status: str | None

    @property
    def pointer_key(self) -> tuple[str, str, int, str]:
        return (
            self.sysid,
            self.flow_id,
            self.action_index,
            self.action_command,
        )


@dataclass(frozen=True)
class FailureRecoveryTarget(FailurePointerTarget):
    resolver_suggestion: Mapping[str, Any]


@dataclass(frozen=True)
class RecoverySessionRecord:
    recovery_session_id: str
    root_failure_id: str
    current_failure_id: str
    sysid: str
    flow_id: str
    action_index: int
    action_command: str
    pinned_memory_id: str
    pinned_actions: tuple[Mapping[str, Any], ...]
    pinned_actions_hash: str
    recovery_status: str
    recovery_attempts: int
    recovery_max_attempts: int
    recovery_rewind_steps: int
    recovery_run_token: str | None

    @property
    def pointer_key(self) -> tuple[str, str, int, str]:
        return (
            self.sysid,
            self.flow_id,
            self.action_index,
            self.action_command,
        )


class RecoveryRobotSession(Protocol):
    connected: bool
    latest_flow: FlowPointer | None
    latest_commands_frame: CommandsFrame | None
    generation: int

    async def run(self, stop_event: asyncio.Event) -> None: ...

    async def wait_connected(self, timeout_seconds: float) -> None: ...

    async def wait_for_commands(
        self,
        *,
        generation: int,
        timeout_seconds: float,
    ) -> CommandsFrame: ...

    async def request_command(
        self,
        command: str,
        arguments: Mapping[str, Any],
        *,
        timeout_seconds: float,
    ) -> CommandReceipt: ...

    async def wait_for_flow(
        self,
        predicate: Callable[[FlowPointer | None], bool],
        *,
        after_revision: int,
        timeout_seconds: float,
        generation: int | None = None,
    ) -> FlowPointer | None: ...


class RecoveryDatabase(Protocol):
    async def fetch_failure(
        self,
        failure_id: str,
    ) -> Mapping[str, Any] | None: ...

    async def prepare(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        max_attempts: int,
        rewind_steps: int,
    ) -> Mapping[str, Any]: ...

    async def attach(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
    ) -> Mapping[str, Any]: ...

    async def project(
        self,
        *,
        recovery_session_id: str,
    ) -> Mapping[str, Any]: ...

    async def claim(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        lease_seconds: int,
    ) -> Mapping[str, Any]: ...

    async def finish(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        result: str,
        message: str | None,
    ) -> Mapping[str, Any]: ...

    async def expired_attempts(
        self,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def expire(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        message: str | None,
    ) -> Mapping[str, Any]: ...

    async def cold_candidates(
        self,
        *,
        sysids: Sequence[str],
        limit: int,
    ) -> Sequence[str]: ...

    async def mark_non_executable(
        self,
        *,
        failure_id: str,
        message: str,
    ) -> None: ...


class SupabaseRecoveryDatabase:
    """Thin service-role adapter for the authoritative recovery RPCs."""

    def __init__(
        self,
        client: Any,
        *,
        failure_events_table: str = "failure_events",
        recovery_sessions_table: str = "failure_recovery_sessions",
    ) -> None:
        self.client = client
        self.failure_events_table = failure_events_table
        self.recovery_sessions_table = recovery_sessions_table

    async def fetch_failure(
        self,
        failure_id: str,
    ) -> Mapping[str, Any] | None:
        response = await (
            self.client.table(self.failure_events_table)
            .select("*")
            .eq("failure_id", failure_id)
            .limit(1)
            .execute()
        )
        rows = _response_rows(response)
        return dict(rows[0]) if rows else None

    async def prepare(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        max_attempts: int,
        rewind_steps: int,
    ) -> Mapping[str, Any]:
        return await self._rpc_one(
            "prepare_failure_recovery",
            {
                "p_failure_id": failure_id,
                "p_recovery_session_id": recovery_session_id,
                "p_max_attempts": max_attempts,
                "p_rewind_steps": rewind_steps,
            },
        )

    async def attach(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
    ) -> Mapping[str, Any]:
        return await self._rpc_one(
            "attach_failure_recovery_session",
            {
                "p_failure_id": failure_id,
                "p_recovery_session_id": recovery_session_id,
            },
        )

    async def project(
        self,
        *,
        recovery_session_id: str,
    ) -> Mapping[str, Any]:
        return await self._rpc_one(
            "project_failure_recovery_session",
            {"p_recovery_session_id": recovery_session_id},
        )

    async def claim(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        lease_seconds: int,
    ) -> Mapping[str, Any]:
        return await self._rpc_one(
            "claim_failure_recovery_attempt",
            {
                "p_failure_id": failure_id,
                "p_recovery_session_id": recovery_session_id,
                "p_lease_seconds": lease_seconds,
            },
        )

    async def finish(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        result: str,
        message: str | None,
    ) -> Mapping[str, Any]:
        return await self._rpc_one(
            "finish_failure_recovery_attempt",
            {
                "p_failure_id": failure_id,
                "p_recovery_session_id": recovery_session_id,
                "p_run_token": run_token,
                "p_result": result,
                "p_message": message,
            },
        )

    async def expired_attempts(
        self,
        *,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        now = datetime.now(timezone.utc).isoformat()
        response = await (
            self.client.table(self.recovery_sessions_table)
            .select("*")
            .in_("recovery_status", ["running", "awaiting_outcome"])
            .lt("recovery_lease_expires_at", now)
            .order("recovery_lease_expires_at")
            .limit(limit)
            .execute()
        )
        return tuple(dict(row) for row in _response_rows(response))

    async def expire(
        self,
        *,
        failure_id: str,
        recovery_session_id: str,
        run_token: str,
        message: str | None,
    ) -> Mapping[str, Any]:
        return await self._rpc_one(
            "expire_failure_recovery_attempt",
            {
                "p_failure_id": failure_id,
                "p_recovery_session_id": recovery_session_id,
                "p_run_token": run_token,
                "p_message": message,
            },
        )

    async def cold_candidates(
        self,
        *,
        sysids: Sequence[str],
        limit: int,
    ) -> Sequence[str]:
        # Fetch never-sent work plus DB-confirmed failed attempts. ``failed`` is
        # written only after a fresh Flow pauses on the exact same step, so it
        # is safe to consume the remaining pinned retry budget after restart.
        # Active/ambiguous attempts are never returned here.
        response = await (
            self.client.table(self.failure_events_table)
            .select("failure_id,sysid,recovery_status")
            .eq("analysis_status", "completed")
            .eq("matcher_status", "solution_found")
            .in_("sysid", list(sysids))
            .or_(
                "recovery_status.is.null,"
                "recovery_status.in.(ready,failed)"
            )
            .order("created_at")
            .limit(limit)
            .execute()
        )
        candidates: list[str] = []
        for row in _response_rows(response):
            if row.get("recovery_status") not in (None, "ready", "failed"):
                continue
            failure_id = _uuid_text(row.get("failure_id"))
            if failure_id is not None:
                candidates.append(failure_id)
        return tuple(candidates)

    async def mark_non_executable(
        self,
        *,
        failure_id: str,
        message: str,
    ) -> None:
        await (
            self.client.table(self.failure_events_table)
            .update(
                {
                    "matcher_status": "no_solution",
                    "matcher_message": message[:2_000],
                    "resolver_suggestion": None,
                }
            )
            .eq("failure_id", failure_id)
            .eq("matcher_status", "solution_found")
            .execute()
        )

    async def _rpc_one(
        self,
        name: str,
        parameters: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        response = await self.client.rpc(name, dict(parameters)).execute()
        data = getattr(response, "data", None)
        if isinstance(data, Mapping):
            return dict(data)
        if (
            isinstance(data, list)
            and len(data) == 1
            and isinstance(data[0], Mapping)
        ):
            return dict(data[0])
        raise RecoveryContractError(f"{name} returned an invalid session row")


@dataclass
class _ActiveRecovery:
    recovery_session_id: str
    current_failure_id: str


RobotSessionFactory = Callable[..., RecoveryRobotSession]


class RecoveryCoordinator:
    """Coordinate at most one automatic recovery at a time per robot."""

    def __init__(
        self,
        settings: RecoveryExecutionSettings,
        *,
        database: RecoveryDatabase | None = None,
        sessions: Mapping[str, RecoveryRobotSession] | None = None,
        session_factory: RobotSessionFactory = RobotSession,
    ) -> None:
        self.settings = settings
        self.state = RecoveryExecutionState()
        self._database = database
        self._stop_event = asyncio.Event()
        self._started = False
        self._work_tasks: set[asyncio.Task[Any]] = set()
        self._session_tasks: list[asyncio.Task[Any]] = []
        self._reconcile_task: asyncio.Task[Any] | None = None
        self._reconcile_lock = asyncio.Lock()
        self._pending_failure_ids: set[str] = set()
        # Supabase can deliver the same row again while the matcher is still
        # producing its solution. Keep one coalesced replay marker instead of
        # dropping that edge or growing an unbounded queue of duplicates.
        self._dirty_failure_ids: set[str] = set()
        self._robot_locks = {
            sysid: asyncio.Lock() for sysid in settings.robot_allowlist
        }
        self._active_by_pointer: dict[
            tuple[str, str, int, str], _ActiveRecovery
        ] = {}

        supplied = {
            sysid.strip().upper(): session
            for sysid, session in (sessions or {}).items()
        }
        self._sessions: dict[str, RecoveryRobotSession] = {}
        for sysid in settings.robot_allowlist:
            session = supplied.get(sysid)
            if session is None and settings.enabled:
                session = session_factory(
                    sysid=sysid,
                    cf_access_client_id=settings.cf_access_client_id,
                    cf_access_client_secret=settings.cf_access_client_secret,
                )
            if session is not None:
                self._sessions[sysid] = session

    def set_database(self, database: RecoveryDatabase | None) -> None:
        self._database = database

    def snapshot(self) -> dict[str, Any]:
        snapshot = self.state.snapshot(enabled=self.settings.enabled)
        snapshot["auto_recovery_robots"] = {
            sysid: {
                "connected": session.connected,
                "generation": session.generation,
                "flow_ready": bool(
                    session.connected
                    and session.latest_flow is not None
                    and session.latest_flow.generation == session.generation
                ),
                "commands_ready": bool(
                    session.connected
                    and session.latest_commands_frame is not None
                    and session.latest_commands_frame.generation
                    == session.generation
                ),
            }
            for sysid, session in self._sessions.items()
        }
        return snapshot

    async def start(self) -> None:
        if self._started or not self.settings.enabled:
            return
        self._started = True
        self._stop_event.clear()
        for sysid, session in self._sessions.items():
            self._session_tasks.append(
                asyncio.create_task(
                    session.run(self._stop_event),
                    name=f"auto-recovery-robot-session:{sysid}",
                )
            )
        self._reconcile_task = asyncio.create_task(
            self._reconcile_loop(),
            name="automatic-recovery-reconciliation",
        )

    async def stop(self) -> None:
        if not self._started:
            return
        self._stop_event.set()
        reconcile_task = self._reconcile_task
        self._reconcile_task = None
        if reconcile_task is not None:
            reconcile_task.cancel()
        tasks = tuple(self._work_tasks)
        for task in tasks:
            task.cancel()
        bounded_tasks = tuple(
            task
            for task in (*tasks, reconcile_task)
            if task is not None
        )
        if bounded_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        *bounded_tasks,
                        return_exceptions=True,
                    ),
                    timeout=max(
                        1.0,
                        min(
                            15.0,
                            self.settings.command_timeout_seconds,
                        ),
                    ),
                )
            except TimeoutError:
                for task in bounded_tasks:
                    task.cancel()
        if self._session_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        *self._session_tasks,
                        return_exceptions=True,
                    ),
                    timeout=max(1.0, self.settings.command_timeout_seconds),
                )
            except TimeoutError:
                for task in self._session_tasks:
                    task.cancel()
                await asyncio.gather(
                    *self._session_tasks,
                    return_exceptions=True,
                )
        self._session_tasks.clear()
        self._pending_failure_ids.clear()
        self._dirty_failure_ids.clear()
        self._started = False

    async def _reconcile_loop(self) -> None:
        interval = self.settings.reconcile_interval_seconds
        backoff = interval
        while not self._stop_event.is_set():
            if await _wait_for_stop(self._stop_event, backoff):
                return
            if self._database is None:
                backoff = interval
                continue
            try:
                await self.reconcile()
                backoff = interval
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.state.execution_errors += 1
                self.state.last_error_type = type(error).__name__
                backoff = min(
                    max(interval, backoff * 2),
                    max(interval, self.settings.lease_seconds / 2),
                )

    def notify_failure(self, failure_id: str) -> None:
        if not self.settings.enabled or self._stop_event.is_set():
            return
        canonical = _uuid_text(failure_id)
        if canonical is None:
            self.state.events_skipped += 1
            return
        if canonical in self._pending_failure_ids:
            self._dirty_failure_ids.add(canonical)
            return
        if (
            len(self._pending_failure_ids) >= _MAX_PENDING_FAILURES
        ):
            self.state.events_skipped += 1
            return
        self._pending_failure_ids.add(canonical)
        self.state.events_enqueued += 1
        task = asyncio.create_task(
            self._process_notified(canonical),
            name=f"automatic-recovery:{canonical}",
        )
        self._work_tasks.add(task)
        task.add_done_callback(self._work_tasks.discard)

    async def _process_notified(self, failure_id: str) -> None:
        try:
            await self.process_failure(failure_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.state.execution_errors += 1
            self.state.last_error_type = type(error).__name__
            logger.exception(
                "Automatic recovery processing failed failure_id=%s: %s",
                failure_id,
                error,
            )
        finally:
            self._pending_failure_ids.discard(failure_id)
            replay = failure_id in self._dirty_failure_ids
            self._dirty_failure_ids.discard(failure_id)
            if replay and not self._stop_event.is_set():
                self.notify_failure(failure_id)

    async def reconcile(self) -> None:
        """Expire ambiguous leases and enqueue only never-sent work."""

        if not self.settings.enabled:
            return
        async with self._reconcile_lock:
            database = self._required_database()
            for raw in await database.expired_attempts(
                limit=self.settings.reconcile_limit
            ):
                try:
                    record = parse_recovery_session(
                        raw,
                        require_run_token=True,
                    )
                    await database.expire(
                        failure_id=record.current_failure_id,
                        recovery_session_id=record.recovery_session_id,
                        run_token=record.recovery_run_token or "",
                        message=(
                            "Automatic recovery became unknown after the "
                            "service lost its observation lease. Inspect the "
                            "robot before taking another action."
                        ),
                    )
                    self.state.expired_attempts_reconciled += 1
                    self.state.attempts_unknown += 1
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.state.execution_errors += 1
                    self.state.last_error_type = type(error).__name__

            for failure_id in await database.cold_candidates(
                sysids=self.settings.robot_allowlist,
                limit=self.settings.reconcile_limit,
            ):
                self.notify_failure(failure_id)

    async def process_failure(self, failure_id: str) -> None:
        """Process one candidate; exposed for deterministic focused tests."""

        if not self.settings.enabled:
            return
        database = self._required_database()
        raw = await database.fetch_failure(failure_id)
        if raw is None:
            self.state.events_skipped += 1
            return
        pointer_target = parse_failure_pointer_target(raw)
        if pointer_target.sysid not in self.settings.robot_allowlist:
            self.state.events_skipped += 1
            return

        # Attach a recurrence before matcher work. The active service-carried
        # session, not a second model decision, owns the pinned plan and budget.
        if await self._attach_active_recurrence(database, pointer_target):
            return

        if (
            str(raw.get("analysis_status") or "").strip() != "completed"
            or str(raw.get("matcher_status") or "").strip() != "solution_found"
        ):
            self.state.events_skipped += 1
            return
        target = parse_failure_recovery_target(raw)
        if target.recovery_status not in (None, "ready", "failed"):
            self.state.events_skipped += 1
            return

        robot_lock = self._robot_locks[target.sysid]
        async with robot_lock:
            # Re-read after waiting for the robot lock. A duplicate Realtime
            # signal may now be terminal or may have been attached by the
            # in-flight recovery.
            current = await database.fetch_failure(target.failure_id)
            if current is None:
                self.state.events_skipped += 1
                return
            target = parse_failure_recovery_target(current)
            if await self._attach_active_recurrence(database, target):
                return
            if target.recovery_status not in (None, "ready", "failed"):
                self.state.events_skipped += 1
                return
            try:
                await self._execute_target(target)
            except RecoveryNonExecutableError as error:
                await database.mark_non_executable(
                    failure_id=target.failure_id,
                    message=(
                        "Automatic recovery requires manual handling: "
                        f"{error}"
                    ),
                )
                self.state.events_skipped += 1
                self.state.last_error_type = type(error).__name__
            except (
                RecoveryLiveStateChanged,
                RobotCommandNotSentError,
                RobotCommandOutcomeUnknownError,
                TimeoutError,
            ) as error:
                # A later bounded reconciliation pass may find the robot
                # connected and paused on the exact same step again.
                self.state.events_skipped += 1
                self.state.last_error_type = type(error).__name__
                logger.warning(
                    "Automatic recovery deferred failure_id=%s error=%s: %s",
                    target.failure_id,
                    type(error).__name__,
                    error,
                )

    async def _attach_active_recurrence(
        self,
        database: RecoveryDatabase,
        target: FailurePointerTarget,
    ) -> bool:
        active = self._active_by_pointer.get(target.pointer_key)
        if active is None:
            return False
        if target.recovery_session_id is None:
            await database.attach(
                failure_id=target.failure_id,
                recovery_session_id=active.recovery_session_id,
            )
            active.current_failure_id = target.failure_id
            self.state.recurrences_attached += 1
            return True
        if target.recovery_session_id == active.recovery_session_id:
            if target.failure_id != active.current_failure_id:
                active.current_failure_id = target.failure_id
                self.state.recurrences_attached += 1
            return True
        raise RecoveryContractError(
            "The same robot Flow step is linked to two recovery sessions"
        )

    async def _execute_target(self, target: FailureRecoveryTarget) -> None:
        database = self._required_database()
        session = self._sessions.get(target.sysid)
        if session is None:
            raise RecoveryContractError(
                f"No robot session exists for allowlisted sysid {target.sysid}"
            )

        pointer = await self._fresh_paused_pointer(session, target)
        await session.wait_for_commands(
            generation=pointer.generation,
            timeout_seconds=self.settings.command_timeout_seconds,
        )

        if target.recovery_session_id is None:
            # Before the first prepare, the failure row is the candidate
            # source. The RPC then freezes this exact plan for the session.
            try:
                row_plan = parse_recovery_plan(
                    target.resolver_suggestion.get("actions")
                )
                validate_plan_for_target(row_plan, target)
                validate_plan_for_flow(row_plan, pointer)
            except RecoveryContractError as error:
                raise RecoveryNonExecutableError(str(error)) from error
            self._require_live_execution_state(
                session,
                pointer,
                row_plan,
            )
            recovery_session_id = str(uuid.uuid4())
            prepared_raw = await database.prepare(
                failure_id=target.failure_id,
                recovery_session_id=recovery_session_id,
                max_attempts=self.settings.max_attempts,
                rewind_steps=row_plan.rewind_steps,
            )
            prepared = parse_recovery_session(prepared_raw)
            # The database may find and attach an authoritative prior session
            # for this same robot/Flow/step across a service restart. Its UUID,
            # pinned plan, terminal state, and cumulative budget win over the
            # newly proposed UUID.
            recovery_session_id = prepared.recovery_session_id
            self.state.sessions_prepared += 1
        else:
            recovery_session_id = target.recovery_session_id
            prepared_raw = await database.project(
                recovery_session_id=recovery_session_id,
            )
            prepared = parse_recovery_session(prepared_raw)

        self.state.last_failure_id = target.failure_id
        self.state.last_session_id = recovery_session_id
        if prepared.recovery_status in _TERMINAL_SESSION_STATUSES:
            self.state.events_skipped += 1
            return
        if prepared.recovery_status not in ("ready", "failed"):
            self.state.events_skipped += 1
            return
        if prepared.pointer_key != target.pointer_key:
            raise RecoveryContractError(
                "Prepared recovery session does not match the failure pointer"
            )
        # A ready session can survive a service restart. Validate every raw
        # pinned action before claiming its first/next attempt; a later failure
        # row suggestion is never an execution source for this session.
        try:
            prepared_plan = parse_recovery_plan(prepared.pinned_actions)
            validate_plan_for_session(prepared_plan, prepared)
            validate_plan_for_flow(prepared_plan, pointer)
            if prepared_plan.rewind_steps != prepared.recovery_rewind_steps:
                raise RecoveryContractError(
                    "Pinned recovery continuation does not match its "
                    "authoritative rewind setting"
                )
        except RecoveryContractError as error:
            raise RecoveryNonExecutableError(str(error)) from error
        # Modern continuations are replayed byte-for-byte from the pinned
        # action. Validate those immutable arguments against this fresh pointer
        # before claim. Only legacy $rerun is upgraded to a newly guarded
        # retry_current payload.
        try:
            resume_arguments_for_plan(prepared_plan, pointer)
        except RecoveryContractError as error:
            raise RecoveryNonExecutableError(str(error)) from error
        self._require_live_execution_state(
            session,
            pointer,
            prepared_plan,
        )
        required_lease_seconds = (
            (len(prepared_plan.corrections) + 1)
            * self.settings.command_timeout_seconds
            + self.settings.outcome_timeout_seconds
            + 5.0
        )
        if self.settings.lease_seconds < math.ceil(required_lease_seconds):
            raise RecoveryContractError(
                "RECOVERY_LEASE_SECONDS is shorter than this pinned plan's "
                "worst-case command and Flow observation window"
            )

        if self.settings.start_grace_seconds > 0:
            # Operator cancellation window: the session is already visible as
            # "ready" through the mirrored failure row, so hold the first
            # claim long enough for a human to cancel it. The claim RPC's
            # ready/failed guard is the atomic backstop for late cancels.
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.settings.start_grace_seconds,
                )
            except asyncio.TimeoutError:
                pass
            if self._stop_event.is_set():
                return
            graced = parse_recovery_session(
                await database.project(
                    recovery_session_id=recovery_session_id,
                )
            )
            if graced.recovery_status not in ("ready", "failed"):
                self.state.events_skipped += 1
                return

        active = _ActiveRecovery(
            recovery_session_id=recovery_session_id,
            current_failure_id=target.failure_id,
        )
        self._active_by_pointer[target.pointer_key] = active
        try:
            while not self._stop_event.is_set():
                current_failure_id = active.current_failure_id
                self._require_live_execution_state(
                    session,
                    pointer,
                    prepared_plan,
                )
                claimed_raw = await database.claim(
                    failure_id=current_failure_id,
                    recovery_session_id=recovery_session_id,
                    lease_seconds=self.settings.lease_seconds,
                )
                self.state.attempts_claimed += 1
                try:
                    claimed = parse_recovery_session(
                        claimed_raw,
                        require_run_token=True,
                    )
                except RecoveryContractError:
                    # Claim may already have incremented the durable attempt.
                    # If its token envelope is intact, close it as unknown
                    # rather than leaving it active until lease reconciliation.
                    claimed_session_id = _uuid_text(
                        claimed_raw.get("recovery_session_id")
                    )
                    claimed_run_token = _uuid_text(
                        claimed_raw.get("recovery_run_token")
                    )
                    if (
                        claimed_session_id == recovery_session_id
                        and claimed_run_token is not None
                    ):
                        await database.finish(
                            failure_id=current_failure_id,
                            recovery_session_id=recovery_session_id,
                            run_token=claimed_run_token,
                            result="unknown",
                            message=(
                                "Automatic recovery claim data was malformed; "
                                "the attempt was stopped as unknown."
                            ),
                        )
                        self.state.attempts_unknown += 1
                        return
                    raise
                self.state.last_failure_id = current_failure_id
                plan = parse_recovery_plan(claimed.pinned_actions)
                if plan.rewind_steps != claimed.recovery_rewind_steps:
                    await self._finish_unknown(
                        claimed,
                        current_failure_id,
                        "Pinned recovery continuation does not match its "
                        "authoritative rewind setting.",
                    )
                    return
                validate_plan_for_session(plan, claimed)
                try:
                    validate_plan_for_flow(plan, pointer)
                    self._require_live_execution_state(
                        session,
                        pointer,
                        plan,
                    )
                except RecoveryContractError:
                    await self._finish_unknown(
                        claimed,
                        current_failure_id,
                        "Automatic recovery stopped before dispatch because "
                        "the live paused Flow pointer changed.",
                    )
                    return

                result, outcome_pointer = await self._execute_attempt(
                    database=database,
                    robot_session=session,
                    claimed=claimed,
                    failure_id=current_failure_id,
                    plan=plan,
                    pointer=pointer,
                )
                if result == "unknown":
                    return
                if result == "recovered":
                    return
                if result != "failed":
                    raise AssertionError(f"Unexpected recovery result {result}")

                finished_record = outcome_pointer
                if not isinstance(finished_record, RecoverySessionRecord):
                    raise AssertionError("Failed result omitted session record")
                if finished_record.recovery_status == "timed_out":
                    return
                if finished_record.recovery_status != "failed":
                    return

                # The only automatic retry path is a fresh, exact recurrence
                # of the same paused failed step. Reuse the DB-pinned plan and
                # cumulative session budget; do not re-read matcher output.
                latest = session.latest_flow
                if latest is None or not _pointer_matches_session(
                    latest,
                    claimed,
                ):
                    return
                pointer = latest
        finally:
            if self._active_by_pointer.get(target.pointer_key) is active:
                self._active_by_pointer.pop(target.pointer_key, None)

    async def _execute_attempt(
        self,
        *,
        database: RecoveryDatabase,
        robot_session: RecoveryRobotSession,
        claimed: RecoverySessionRecord,
        failure_id: str,
        plan: RecoveryPlan,
        pointer: FlowPointer,
    ) -> tuple[str, FlowPointer | RecoverySessionRecord | None]:
        run_token = claimed.recovery_run_token
        if run_token is None:
            raise RecoveryContractError("Claimed attempt has no run token")
        command_dispatched = False
        try:
            for action in plan.corrections:
                self._require_live_execution_state(
                    robot_session,
                    pointer,
                    plan,
                )
                command_dispatched = True
                receipt = await robot_session.request_command(
                    action.command,
                    action.arguments,
                    timeout_seconds=self.settings.command_timeout_seconds,
                )
                if receipt.generation != pointer.generation:
                    raise RecoveryContractError(
                        "correction acknowledgement came from another "
                        "robot connection"
                    )
                self.state.actions_accepted += 1

            self._require_live_execution_state(
                robot_session,
                pointer,
                plan,
            )
            resume_arguments = resume_arguments_for_plan(plan, pointer)
            command_dispatched = True
            resume_receipt = await robot_session.request_command(
                "$resume_flow",
                resume_arguments,
                timeout_seconds=self.settings.command_timeout_seconds,
            )
            if resume_receipt.generation != pointer.generation:
                raise RecoveryContractError(
                    "continuation acknowledgement came from another "
                    "robot connection"
                )
            validate_resume_acknowledgement(
                resume_receipt.result,
                pointer=pointer,
                rewind_steps=plan.rewind_steps,
                mode=plan.resume_mode,
            )
            self.state.actions_accepted += 1

            await database.finish(
                failure_id=failure_id,
                recovery_session_id=claimed.recovery_session_id,
                run_token=run_token,
                result="awaiting_outcome",
                message=(
                    f"Automatic recovery attempt "
                    f"{claimed.recovery_attempts} of "
                    f"{claimed.recovery_max_attempts} was accepted; "
                    "waiting for the Flow to pass the failed step."
                ),
            )
            outcome = await robot_session.wait_for_flow(
                _execution_outcome_predicate(
                    claimed,
                    generation=pointer.generation,
                ),
                after_revision=(
                    resume_receipt.flow_revision_at_correlated_ack
                ),
                timeout_seconds=self.settings.outcome_timeout_seconds,
                generation=pointer.generation,
            )
            classification = classify_flow_outcome(outcome, claimed)
            if classification == "recovered":
                finished = parse_recovery_session(
                    await database.finish(
                        failure_id=failure_id,
                        recovery_session_id=claimed.recovery_session_id,
                        run_token=run_token,
                        result="recovered",
                        message=(
                            "Automatic recovery succeeded: the Flow advanced "
                            "past the failed step."
                        ),
                    )
                )
                self.state.attempts_recovered += 1
                return "recovered", finished
            if classification == "failed":
                finished = parse_recovery_session(
                    await database.finish(
                        failure_id=failure_id,
                        recovery_session_id=claimed.recovery_session_id,
                        run_token=run_token,
                        result="failed",
                        message=(
                            "The same Flow step paused again after automatic "
                            "recovery."
                        ),
                    )
                )
                self._record_failed(finished)
                return "failed", finished

            await self._finish_unknown(
                claimed,
                failure_id,
                "The Flow changed in a way that could not be safely "
                "classified as recovery or the same failure.",
            )
            return "unknown", outcome
        except asyncio.CancelledError:
            if command_dispatched:
                finish_task = asyncio.create_task(
                    self._finish_unknown(
                        claimed,
                        failure_id,
                        "Automatic recovery was interrupted after dispatch; "
                        "the robot outcome is unknown.",
                    )
                )
                try:
                    await asyncio.wait_for(
                        asyncio.shield(finish_task),
                        timeout=max(
                            0.1,
                            min(
                                5.0,
                                self.settings.command_timeout_seconds,
                            ),
                        ),
                    )
                except (asyncio.CancelledError, TimeoutError):
                    finish_task.cancel()
            raise
        except (
            RobotCommandNotSentError,
            RobotCommandOutcomeUnknownError,
            TimeoutError,
        ) as error:
            await self._finish_unknown(
                claimed,
                failure_id,
                (
                    "Automatic recovery lost command or Flow confirmation; "
                    "the robot outcome is unknown."
                ),
            )
            self.state.last_error_type = type(error).__name__
            return "unknown", None
        except RobotCommandRejectedError as error:
            # ``failed`` is intentionally retryable and is reserved for a
            # freshly observed recurrence of the same Flow step. A rejection
            # has no such evidence, so terminate fail-closed instead.
            await self._finish_unknown(
                claimed,
                failure_id,
                "The robot explicitly rejected an automatic recovery "
                "command; automatic retry was disabled.",
            )
            self.state.last_error_type = type(error).__name__
            return "unknown", None
        except RecoveryContractError as error:
            # A malformed response after dispatch is ambiguous. A malformed
            # pinned claim before dispatch is also terminal because its attempt
            # was already atomically claimed.
            await self._finish_unknown(
                claimed,
                failure_id,
                "Automatic recovery received malformed pinned or robot data; "
                "the attempt was stopped as unknown.",
            )
            self.state.last_error_type = type(error).__name__
            return "unknown", None

    async def _finish_unknown(
        self,
        claimed: RecoverySessionRecord,
        failure_id: str,
        message: str,
    ) -> None:
        run_token = claimed.recovery_run_token
        if run_token is None:
            return
        await self._required_database().finish(
            failure_id=failure_id,
            recovery_session_id=claimed.recovery_session_id,
            run_token=run_token,
            result="unknown",
            message=message,
        )
        self.state.attempts_unknown += 1

    def _record_failed(self, record: RecoverySessionRecord) -> None:
        self.state.attempts_failed += 1
        if record.recovery_status == "timed_out":
            self.state.sessions_timed_out += 1

    async def _fresh_paused_pointer(
        self,
        session: RecoveryRobotSession,
        target: FailureRecoveryTarget,
    ) -> FlowPointer:
        await session.wait_connected(self.settings.command_timeout_seconds)
        generation = session.generation
        latest = session.latest_flow
        if (
            latest is not None
            and latest.generation == generation
            and _pointer_matches_target(latest, target)
        ):
            return latest
        after_revision = (
            latest.revision
            if latest is not None and latest.generation == generation
            else 0
        )
        pointer = await session.wait_for_flow(
            lambda candidate: _pointer_matches_target(candidate, target),
            after_revision=after_revision,
            timeout_seconds=self.settings.outcome_timeout_seconds,
            generation=generation,
        )
        if (
            pointer is None
            or pointer.generation != generation
            or not _pointer_matches_target(pointer, target)
        ):
            raise RecoveryContractError(
                "No fresh exact paused Flow pointer is available"
            )
        return pointer

    def _require_live_execution_state(
        self,
        session: RecoveryRobotSession,
        pointer: FlowPointer,
        plan: RecoveryPlan,
    ) -> CommandsFrame:
        latest = session.latest_flow
        if (
            not session.connected
            or session.generation != pointer.generation
            or latest is None
            or latest.generation != pointer.generation
            or latest.revision != pointer.revision
            or latest.flow_id != pointer.flow_id
            or latest.status != pointer.status
            or latest.action_index != pointer.action_index
            or latest.action_command != pointer.action_command
        ):
            raise RecoveryLiveStateChanged(
                "live paused Flow pointer changed before dispatch"
            )
        frame = session.latest_commands_frame
        if frame is None or frame.generation != pointer.generation:
            raise RecoveryLiveStateChanged(
                "fresh live commands action spec is unavailable"
            )
        try:
            validate_plan_for_flow(plan, latest)
            validate_corrections_against_commands(
                plan.corrections,
                frame,
                generation=pointer.generation,
            )
        except RecoveryContractError as error:
            raise RecoveryNonExecutableError(str(error)) from error
        return frame

    def _required_database(self) -> RecoveryDatabase:
        if self._database is None:
            raise RecoveryDatabaseUnavailable(
                "Supabase recovery control plane is unavailable"
            )
        return self._database


def parse_failure_pointer_target(
    row: Mapping[str, Any],
) -> FailurePointerTarget:
    failure_id = _required_uuid(row.get("failure_id"), "failure_id")
    sysid = _required_text(row.get("sysid"), "sysid", max_length=64).upper()
    if not _SYSID_PATTERN.fullmatch(sysid):
        raise RecoveryContractError("sysid is invalid")
    flow_id = _required_text(row.get("flow_id"), "flow_id", max_length=300)
    action_index = _required_int(
        row.get("action_index"),
        "action_index",
        minimum=0,
        maximum=1_000_000,
    )
    action_command = _required_text(
        row.get("action_command"),
        "action_command",
        max_length=_MAX_COMMAND_LENGTH,
    )
    episode_key = _required_text(
        row.get("recovery_episode_key"),
        "recovery_episode_key",
        max_length=64,
    )
    if not _EPISODE_KEY_PATTERN.fullmatch(episode_key):
        raise RecoveryContractError("recovery_episode_key is invalid")
    status_value = row.get("recovery_status")
    recovery_status: str | None
    if status_value is None:
        recovery_status = None
    else:
        recovery_status = _required_text(
            status_value,
            "recovery_status",
            max_length=32,
        )
        if recovery_status not in _SESSION_STATUSES:
            raise RecoveryContractError("recovery_status is invalid")
    session_id = (
        None
        if row.get("recovery_session_id") is None
        else _required_uuid(
            row.get("recovery_session_id"),
            "recovery_session_id",
        )
    )
    return FailurePointerTarget(
        failure_id=failure_id,
        sysid=sysid,
        flow_id=flow_id,
        action_index=action_index,
        action_command=action_command,
        recovery_episode_key=episode_key,
        recovery_session_id=session_id,
        recovery_status=recovery_status,
    )


def parse_failure_recovery_target(
    row: Mapping[str, Any],
) -> FailureRecoveryTarget:
    target = parse_failure_pointer_target(row)
    if str(row.get("analysis_status") or "").strip() != "completed":
        raise RecoveryContractError("failure analysis is not completed")
    if str(row.get("matcher_status") or "").strip() != "solution_found":
        raise RecoveryContractError("failure has no pinned solution candidate")
    suggestion = row.get("resolver_suggestion")
    if not isinstance(suggestion, Mapping):
        raise RecoveryContractError("resolver_suggestion must be an object")
    _required_text(
        suggestion.get("memory_id"),
        "resolver_suggestion.memory_id",
        max_length=300,
    )
    return FailureRecoveryTarget(
        failure_id=target.failure_id,
        sysid=target.sysid,
        flow_id=target.flow_id,
        action_index=target.action_index,
        action_command=target.action_command,
        recovery_episode_key=target.recovery_episode_key,
        recovery_session_id=target.recovery_session_id,
        recovery_status=target.recovery_status,
        resolver_suggestion=copy.deepcopy(dict(suggestion)),
    )


def parse_recovery_session(
    row: Mapping[str, Any],
    *,
    require_run_token: bool = False,
) -> RecoverySessionRecord:
    status = _required_text(
        row.get("recovery_status"),
        "recovery_status",
        max_length=32,
    )
    if status not in _SESSION_STATUSES:
        raise RecoveryContractError("recovery_status is invalid")
    actions = row.get("pinned_actions")
    if not isinstance(actions, list) or not 1 <= len(actions) <= _MAX_ACTIONS:
        raise RecoveryContractError("pinned_actions must contain 1 to 10 actions")
    if not all(isinstance(action, Mapping) for action in actions):
        raise RecoveryContractError("pinned_actions contains a non-object")
    attempts = _required_int(
        row.get("recovery_attempts"),
        "recovery_attempts",
        minimum=0,
        maximum=20,
    )
    max_attempts = _required_int(
        row.get("recovery_max_attempts"),
        "recovery_max_attempts",
        minimum=1,
        maximum=20,
    )
    if attempts > max_attempts:
        raise RecoveryContractError("recovery_attempts exceeds its maximum")
    rewind_steps = _required_int(
        row.get("recovery_rewind_steps"),
        "recovery_rewind_steps",
        minimum=0,
        maximum=_MAX_REWIND_STEPS,
    )
    run_token = (
        None
        if row.get("recovery_run_token") is None
        else _required_uuid(
            row.get("recovery_run_token"),
            "recovery_run_token",
        )
    )
    if require_run_token and (status not in ("running", "awaiting_outcome")):
        raise RecoveryContractError("claimed session is not active")
    if require_run_token and run_token is None:
        raise RecoveryContractError("claimed session has no run token")
    if status == "ready" and (attempts != 0 or run_token is not None):
        raise RecoveryContractError("ready recovery session lifecycle is invalid")
    if status in ("running", "awaiting_outcome") and (
        attempts < 1 or run_token is None
    ):
        raise RecoveryContractError("active recovery session lifecycle is invalid")
    if status == "failed" and (
        attempts < 1 or attempts >= max_attempts or run_token is None
    ):
        raise RecoveryContractError("failed recovery session lifecycle is invalid")
    if status in ("recovered", "unknown") and (
        attempts < 1 or run_token is None
    ):
        raise RecoveryContractError("terminal recovery session lifecycle is invalid")
    if status == "timed_out" and (
        attempts != max_attempts or run_token is None
    ):
        raise RecoveryContractError("timed_out recovery lifecycle is invalid")
    actions_hash = _required_text(
        row.get("pinned_actions_hash"),
        "pinned_actions_hash",
        max_length=64,
    )
    if not _HASH_PATTERN.fullmatch(actions_hash):
        raise RecoveryContractError("pinned_actions_hash is invalid")
    pinned_memory_id = _required_text(
        row.get("pinned_memory_id"),
        "pinned_memory_id",
        max_length=300,
    )
    suggestion = row.get("pinned_resolver_suggestion")
    if not isinstance(suggestion, Mapping):
        raise RecoveryContractError("pinned_resolver_suggestion is invalid")
    if suggestion.get("memory_id") != pinned_memory_id:
        raise RecoveryContractError("pinned memory identity does not match")
    if suggestion.get("actions") != actions:
        raise RecoveryContractError("pinned action copies do not match")
    root_episode_key = _required_text(
        row.get("root_episode_key"),
        "root_episode_key",
        max_length=64,
    )
    if not _EPISODE_KEY_PATTERN.fullmatch(root_episode_key):
        raise RecoveryContractError("root_episode_key is invalid")
    sysid = _required_text(row.get("sysid"), "sysid", max_length=64).upper()
    if not _SYSID_PATTERN.fullmatch(sysid):
        raise RecoveryContractError("recovery session sysid is invalid")
    return RecoverySessionRecord(
        recovery_session_id=_required_uuid(
            row.get("recovery_session_id"),
            "recovery_session_id",
        ),
        root_failure_id=_required_uuid(
            row.get("root_failure_id"),
            "root_failure_id",
        ),
        current_failure_id=_required_uuid(
            row.get("current_failure_id"),
            "current_failure_id",
        ),
        sysid=sysid,
        flow_id=_required_text(
            row.get("flow_id"),
            "flow_id",
            max_length=300,
        ),
        action_index=_required_int(
            row.get("action_index"),
            "action_index",
            minimum=0,
            maximum=1_000_000,
        ),
        action_command=_required_text(
            row.get("action_command"),
            "action_command",
            max_length=_MAX_COMMAND_LENGTH,
        ),
        pinned_memory_id=pinned_memory_id,
        pinned_actions=tuple(copy.deepcopy(dict(action)) for action in actions),
        pinned_actions_hash=actions_hash,
        recovery_status=status,
        recovery_attempts=attempts,
        recovery_max_attempts=max_attempts,
        recovery_rewind_steps=rewind_steps,
        recovery_run_token=run_token,
    )


def parse_recovery_plan(value: Any) -> RecoveryPlan:
    if not isinstance(value, (list, tuple)):
        raise RecoveryContractError("recovery actions must be an array")
    if not 1 <= len(value) <= _MAX_ACTIONS:
        raise RecoveryContractError("recovery actions must contain 1 to 10 items")

    parsed: list[tuple[str, Mapping[str, Any], Mapping[str, Any]]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise RecoveryContractError(f"recovery action {index} is not an object")
        unknown = set(raw) - _ALLOWED_ACTION_FIELDS
        if unknown:
            raise RecoveryContractError(
                f"recovery action {index} has unsupported fields"
            )
        command = _required_text(
            raw.get("command"),
            f"recovery action {index}.command",
            max_length=_MAX_COMMAND_LENGTH,
        )
        if "title" in raw:
            _required_text(
                raw.get("title"),
                f"recovery action {index}.title",
                max_length=300,
            )
        arguments = raw.get("arguments")
        if not isinstance(arguments, Mapping):
            raise RecoveryContractError(
                f"recovery action {index}.arguments must be an object"
            )
        arguments_copy = copy.deepcopy(dict(arguments))
        _validate_json(arguments_copy)
        if len(
            json.dumps(
                arguments_copy,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ) > _MAX_ARGUMENT_BYTES:
            raise RecoveryContractError(
                f"recovery action {index}.arguments is too large"
            )
        if "arguments_effective" in raw:
            effective = raw.get("arguments_effective")
            if not isinstance(effective, Mapping):
                raise RecoveryContractError(
                    f"recovery action {index}.arguments_effective is invalid"
                )
            _validate_json(effective)
            explicit_keys = raw.get(
                "explicit_arguments",
                list(arguments_copy),
            )
            if not isinstance(explicit_keys, list) or any(
                key not in effective
                or not _json_values_equal(
                    effective[key],
                    arguments_copy.get(key),
                )
                for key in explicit_keys
            ):
                raise RecoveryContractError(
                    f"recovery action {index} argument provenance is invalid"
                )
        if "explicit_arguments" in raw:
            explicit = raw.get("explicit_arguments")
            if (
                not isinstance(explicit, list)
                or not all(isinstance(item, str) for item in explicit)
                or len(set(explicit)) != len(explicit)
                or any(
                    not item
                    or item.strip() != item
                    or len(item) > _MAX_COMMAND_LENGTH
                    for item in explicit
                )
                or set(explicit) != set(arguments_copy)
            ):
                raise RecoveryContractError(
                    f"recovery action {index}.explicit_arguments is invalid"
                )
        parsed.append((command, arguments_copy, raw))

    terminal_positions = [
        index
        for index, (command, _arguments, _raw) in enumerate(parsed)
        if command in _TERMINAL_COMMANDS
    ]
    if terminal_positions != [len(parsed) - 1]:
        raise RecoveryContractError(
            "recovery requires exactly one terminal $rerun or $resume_flow"
        )
    for command, _arguments, raw in parsed[:-1]:
        if command.startswith("$"):
            raise RecoveryContractError(
                "meta-commands are forbidden before the terminal continuation"
            )
        if (
            raw.get("retry_context") is not None
            or raw.get("continuation_context") is not None
        ):
            raise RecoveryContractError(
                "correction actions cannot carry Flow continuation context"
            )

    terminal_command, terminal_arguments, terminal_raw = parsed[-1]
    corrections = tuple(
        RecoveryAction(
            command=command,
            arguments=arguments,
            arguments_effective=(
                copy.deepcopy(dict(raw["arguments_effective"]))
                if isinstance(raw.get("arguments_effective"), Mapping)
                else None
            ),
            explicit_arguments=(
                tuple(raw["explicit_arguments"])
                if isinstance(raw.get("explicit_arguments"), list)
                else None
            ),
        )
        for command, arguments, raw in parsed[:-1]
    )
    if terminal_command == "$rerun":
        if terminal_raw.get("continuation_context") is not None:
            raise RecoveryContractError(
                "$rerun cannot carry continuation_context"
            )
        if terminal_arguments:
            raise RecoveryContractError("$rerun arguments must be empty")
        retry_context = terminal_raw.get("retry_context")
        retried_action = (
            retry_context.get("retried_action")
            if isinstance(retry_context, Mapping)
            else None
        )
        if not isinstance(retried_action, Mapping):
            raise RecoveryContractError("$rerun requires retry_context")
        if set(retry_context) - {"retried_action", "expected_next_action"}:
            raise RecoveryContractError("$rerun retry_context is invalid")
        if "expected_next_action" not in retry_context:
            raise RecoveryContractError(
                "$rerun requires expected_next_action context"
            )
        retry_action = _parse_continuation_action(
            retried_action,
            "retry_context.retried_action",
        )
        expected_next_raw = retry_context.get("expected_next_action")
        expected_next = (
            None
            if expected_next_raw is None
            else _parse_continuation_action(
                expected_next_raw,
                "retry_context.expected_next_action",
            )
        )
        return RecoveryPlan(
            corrections=corrections,
            terminal_command="$rerun",
            terminal_arguments={},
            rewind_steps=0,
            resume_mode="retry_current",
            legacy_retry_index=retry_action.action_index,
            legacy_retry_command=retry_action.command,
            legacy_retry_context=retry_action,
            legacy_expected_next=expected_next,
            legacy_expected_next_present=True,
        )

    unknown_arguments = set(terminal_arguments) - _ALLOWED_RESUME_ARGUMENTS
    if unknown_arguments:
        raise RecoveryContractError("$resume_flow has unsupported arguments")
    mode = terminal_arguments.get("mode")
    if mode not in ("retry_current", "rewind"):
        raise RecoveryContractError("$resume_flow mode is invalid")
    rewind_steps = _required_int(
        terminal_arguments.get("rewind_steps"),
        "$resume_flow.rewind_steps",
        minimum=0,
        maximum=_MAX_REWIND_STEPS,
    )
    if mode == "retry_current" and rewind_steps != 0:
        raise RecoveryContractError("retry_current requires rewind_steps=0")
    if mode == "rewind" and rewind_steps == 0:
        raise RecoveryContractError("rewind requires between 1 and 5 steps")
    _required_text(
        terminal_arguments.get("flow_id"),
        "$resume_flow.flow_id",
        max_length=300,
    )
    for key in ("filename", "fid"):
        if key in terminal_arguments:
            _required_text(
                terminal_arguments.get(key),
                f"$resume_flow.{key}",
                max_length=300,
            )
    if (
        "filename" in terminal_arguments
        and "fid" in terminal_arguments
        and terminal_arguments["filename"] != terminal_arguments["fid"]
    ):
        raise RecoveryContractError(
            "$resume_flow filename and fid must match"
        )
    if (
        "flow_commit" in terminal_arguments
        and terminal_arguments.get("flow_commit") is not None
    ):
        _required_text(
            terminal_arguments.get("flow_commit"),
            "$resume_flow.flow_commit",
            max_length=300,
        )
    captured_index = _required_int(
        terminal_arguments.get("current_action_index"),
        "$resume_flow.current_action_index",
        minimum=0,
        maximum=1_000_000,
    )
    captured_command = _required_text(
        terminal_arguments.get("current_command"),
        "$resume_flow.current_command",
        max_length=_MAX_COMMAND_LENGTH,
    )
    continuation = terminal_raw.get("continuation_context")
    if not isinstance(continuation, Mapping):
        raise RecoveryContractError("$resume_flow requires continuation_context")
    if set(continuation) - {
        "expected_arguments",
        "current_action",
        "target_action",
    }:
        raise RecoveryContractError("continuation_context is invalid")
    expected_arguments = continuation.get("expected_arguments")
    if (
        not isinstance(expected_arguments, Mapping)
        or dict(expected_arguments) != dict(terminal_arguments)
    ):
        raise RecoveryContractError(
            "continuation_context does not match $resume_flow arguments"
        )
    if terminal_raw.get("retry_context") is not None:
        raise RecoveryContractError("$resume_flow cannot carry retry_context")
    current_context = continuation.get("current_action")
    target_context = continuation.get("target_action")
    current = _parse_continuation_action(
        current_context,
        "continuation_context.current_action",
    )
    target = _parse_continuation_action(
        target_context,
        "continuation_context.target_action",
    )
    if (
        current.action_index != captured_index
        or current.command != captured_command
        or target.action_index != captured_index - rewind_steps
    ):
        raise RecoveryContractError(
            "continuation_context action pointers do not match "
            "$resume_flow arguments"
        )
    if mode == "retry_current" and target != current:
        raise RecoveryContractError(
            "retry_current continuation target must equal the current action"
        )
    return RecoveryPlan(
        corrections=corrections,
        terminal_command="$resume_flow",
        terminal_arguments=copy.deepcopy(dict(terminal_arguments)),
        rewind_steps=rewind_steps,
        resume_mode=mode,
        captured_action_index=captured_index,
        captured_action_command=captured_command,
        captured_current_context=current,
        captured_target_context=target,
    )


def validate_plan_for_target(
    plan: RecoveryPlan,
    target: FailureRecoveryTarget,
) -> None:
    expected_index = (
        plan.legacy_retry_index
        if plan.legacy_retry_index is not None
        else plan.captured_action_index
    )
    expected_command = (
        plan.legacy_retry_command
        if plan.legacy_retry_command is not None
        else plan.captured_action_command
    )
    if (
        expected_index != target.action_index
        or expected_command != target.action_command
    ):
        raise RecoveryContractError(
            "Stored continuation does not identify the failed Flow step"
        )


def validate_plan_for_session(
    plan: RecoveryPlan,
    session: RecoverySessionRecord,
) -> None:
    expected_index = (
        plan.legacy_retry_index
        if plan.legacy_retry_index is not None
        else plan.captured_action_index
    )
    expected_command = (
        plan.legacy_retry_command
        if plan.legacy_retry_command is not None
        else plan.captured_action_command
    )
    if (
        expected_index != session.action_index
        or expected_command != session.action_command
    ):
        raise RecoveryContractError(
            "Pinned continuation does not identify the recovery session step"
        )


def flow_action_contexts(
    flow: Mapping[str, Any],
) -> tuple[FlowActionContext, ...]:
    areas = flow.get("areas")
    if not isinstance(areas, list):
        raise RecoveryContractError("live Flow has no action tree")
    contexts: list[FlowActionContext] = []
    seen_indices: set[int] = set()
    for area in areas:
        if not isinstance(area, Mapping) or area.get("_disabled") is True:
            continue
        area_name = _required_text(
            area.get("name"),
            "Flow area.name",
            max_length=300,
        )
        items = area.get("items")
        if not isinstance(items, list):
            raise RecoveryContractError("live Flow area has invalid items")
        for item in items:
            if not isinstance(item, Mapping) or item.get("_disabled") is True:
                continue
            item_name = _required_text(
                item.get("name"),
                "Flow item.name",
                max_length=300,
            )
            actions = item.get("actions")
            if not isinstance(actions, list):
                raise RecoveryContractError(
                    "live Flow item has invalid actions"
                )
            for action in actions:
                if (
                    not isinstance(action, Mapping)
                    or action.get("_disabled") is True
                ):
                    continue
                action_index = _required_int(
                    action.get("action_index"),
                    "Flow action.action_index",
                    minimum=0,
                    maximum=1_000_000,
                )
                if action_index in seen_indices:
                    raise RecoveryContractError(
                        "live Flow action indices are ambiguous"
                    )
                seen_indices.add(action_index)
                contexts.append(
                    FlowActionContext(
                        action_index=action_index,
                        command=_required_text(
                            action.get("command"),
                            "Flow action.command",
                            max_length=_MAX_COMMAND_LENGTH,
                        ),
                        area_name=area_name,
                        item_name=item_name,
                    )
                )
    return tuple(contexts)


def validate_plan_for_flow(
    plan: RecoveryPlan,
    pointer: FlowPointer,
) -> None:
    if (
        pointer.completed
        or pointer.action_index is None
        or pointer.action_command is None
    ):
        raise RecoveryContractError(
            "recovery requires a live non-terminal Flow action"
        )
    contexts = flow_action_contexts(pointer.raw)
    by_index = {context.action_index: context for context in contexts}
    live_current = by_index.get(pointer.action_index)
    if (
        live_current is None
        or live_current.command != pointer.action_command
    ):
        raise RecoveryContractError(
            "live Flow pointer does not match its action tree"
        )

    if plan.terminal_command == "$resume_flow":
        current = plan.captured_current_context
        target = plan.captured_target_context
        live_target = by_index.get(pointer.action_index - plan.rewind_steps)
        if current != live_current or target != live_target:
            raise RecoveryContractError(
                "Pinned continuation structure differs from the live Flow"
            )
        return

    retry = plan.legacy_retry_context
    if (
        plan.terminal_command != "$rerun"
        or retry != live_current
        or not plan.legacy_expected_next_present
    ):
        raise RecoveryContractError(
            "Pinned retry structure differs from the live Flow"
        )
    position = contexts.index(live_current)
    live_next = (
        contexts[position + 1] if position + 1 < len(contexts) else None
    )
    if plan.legacy_expected_next != live_next:
        raise RecoveryContractError(
            "The action following the pinned retry has changed"
        )


def rebind_recovery_actions(
    actions: Sequence[Mapping[str, Any]],
    *,
    flow_snapshot: Mapping[str, Any],
    flow_id: str,
    action_index: int,
    action_command: str,
) -> tuple[Mapping[str, Any], ...]:
    """Bind generic memory evidence to one exact captured failure Flow."""

    pointer = parse_flow_pointer(
        flow_snapshot,
        revision=0,
        generation=0,
    )
    if (
        pointer is None
        or pointer.completed
        or pointer.status.strip().casefold() != "paused"
        or pointer.flow_id != flow_id
        or pointer.action_index != action_index
        or pointer.action_command != action_command
    ):
        raise RecoveryContractError(
            "failure Flow snapshot does not match its captured pointer"
        )

    plan = parse_recovery_plan(actions)
    copied = [copy.deepcopy(dict(action)) for action in actions]
    if plan.terminal_command == "$rerun":
        validate_plan_for_flow(plan, pointer)
        return tuple(copied)

    remembered_current = plan.captured_current_context
    remembered_target = plan.captured_target_context
    if remembered_current is None or remembered_target is None:
        raise RecoveryContractError(
            "remembered continuation has no structural context"
        )
    contexts = flow_action_contexts(pointer.raw)
    by_index = {context.action_index: context for context in contexts}
    live_current = by_index.get(action_index)
    live_target = by_index.get(action_index - plan.rewind_steps)
    if (
        live_current is None
        or live_target is None
        or (
            remembered_current.command,
            remembered_current.area_name,
            remembered_current.item_name,
        )
        != (
            live_current.command,
            live_current.area_name,
            live_current.item_name,
        )
        or (
            remembered_target.command,
            remembered_target.area_name,
            remembered_target.item_name,
        )
        != (
            live_target.command,
            live_target.area_name,
            live_target.item_name,
        )
        or (
            remembered_current.action_index
            - remembered_target.action_index
        )
        != plan.rewind_steps
    ):
        raise RecoveryContractError(
            "remembered continuation structure differs from this Flow run"
        )

    fresh_arguments = dict(
        build_guarded_resume_arguments(
            pointer,
            rewind_steps=plan.rewind_steps,
            mode=plan.resume_mode,
        )
    )
    terminal = copied[-1]
    terminal["arguments"] = copy.deepcopy(fresh_arguments)
    if "arguments_effective" in terminal:
        terminal["arguments_effective"] = copy.deepcopy(fresh_arguments)
    if "explicit_arguments" in terminal:
        terminal["explicit_arguments"] = list(fresh_arguments)
    terminal["continuation_context"] = {
        "expected_arguments": copy.deepcopy(fresh_arguments),
        "current_action": _context_payload(live_current),
        "target_action": _context_payload(live_target),
    }

    rebound = tuple(copied)
    validate_plan_for_flow(parse_recovery_plan(rebound), pointer)
    return rebound


def _context_payload(context: FlowActionContext) -> dict[str, Any]:
    return {
        "actionIndex": context.action_index,
        "command": context.command,
        "areaName": context.area_name,
        "itemName": context.item_name,
    }


def validate_corrections_against_commands(
    corrections: Sequence[RecoveryAction],
    frame: CommandsFrame,
    *,
    generation: int,
) -> None:
    if frame.generation != generation:
        raise RecoveryContractError(
            "commands action spec belongs to an old robot connection"
        )
    if not isinstance(frame.commands, Mapping):
        raise RecoveryContractError("commands action spec is invalid")
    for action in corrections:
        schema = frame.commands.get(action.command)
        if not isinstance(schema, Mapping):
            raise RecoveryContractError(
                f"Command {action.command!r} is not in the live action spec"
            )
        if _command_requires_confirmation(
            action.command,
            action.arguments,
            schema,
        ):
            raise RecoveryContractError(
                f"Command {action.command!r} requires operator confirmation"
            )
        _validate_action_arguments(
            action.command,
            action.arguments,
            schema,
        )
        _validate_action_provenance(action, schema)


def _command_requires_confirmation(
    command: str,
    arguments: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> bool:
    if schema.get("requires_confirmation") is True:
        return True
    if command in _CONFIRMED_COMMANDS:
        return True
    if command == _CONDITIONAL_CONFIRMATION_COMMAND:
        return arguments.get("allow_download") is not False
    return False


def _validate_action_arguments(
    command: str,
    arguments: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> None:
    schema_name = schema.get("name")
    if schema_name is not None and schema_name != command:
        raise RecoveryContractError(
            f"Live action spec name differs for {command!r}"
        )
    inputs = schema.get("inputs")
    if not isinstance(inputs, list):
        raise RecoveryContractError(
            f"Live action spec inputs are invalid for {command!r}"
        )

    by_path: dict[tuple[str, ...], Mapping[str, Any]] = {}
    for raw_input in inputs:
        if not isinstance(raw_input, Mapping):
            raise RecoveryContractError(
                f"Live action spec input is invalid for {command!r}"
            )
        name = raw_input.get("name")
        if (
            not isinstance(name, str)
            or not name
            or name.strip() != name
        ):
            raise RecoveryContractError(
                f"Live action spec input name is invalid for {command!r}"
            )
        path = tuple(name.split("."))
        if any(not part for part in path) or path in by_path:
            raise RecoveryContractError(
                f"Live action spec input paths are ambiguous for {command!r}"
            )
        if not isinstance(raw_input.get("required"), bool):
            raise RecoveryContractError(
                f"Live action spec required flag is invalid for {command!r}"
            )
        by_path[path] = raw_input

    paths = tuple(by_path)
    for path in paths:
        if any(
            other != path
            and len(other) < len(path)
            and path[: len(other)] == other
            for other in paths
        ):
            raise RecoveryContractError(
                f"Live action spec paths overlap for {command!r}"
            )

    def validate_object(
        value: Mapping[str, Any],
        prefix: tuple[str, ...],
    ) -> None:
        for key, argument_value in value.items():
            if not isinstance(key, str) or not key:
                raise RecoveryContractError(
                    f"Arguments for {command!r} contain an invalid name"
                )
            candidate = prefix + (key,)
            input_schema = by_path.get(candidate)
            descendants = tuple(
                path
                for path in paths
                if len(path) > len(candidate)
                and path[: len(candidate)] == candidate
            )
            if input_schema is not None:
                _validate_input_value(
                    argument_value,
                    input_schema,
                    f"{command}.{'.'.join(candidate)}",
                )
            elif descendants:
                if not isinstance(argument_value, Mapping):
                    raise RecoveryContractError(
                        f"{command}.{'.'.join(candidate)} must be an object"
                    )
                validate_object(argument_value, candidate)
            else:
                raise RecoveryContractError(
                    f"Argument {'.'.join(candidate)!r} is not in the live "
                    f"spec for {command!r}"
                )

    validate_object(arguments, ())
    for path, input_schema in by_path.items():
        value = _path_value(arguments, path)
        if input_schema["required"] and (
            value is _MISSING
            or value is None
            or (isinstance(value, str) and not value)
        ):
            raise RecoveryContractError(
                f"Required argument {'.'.join(path)!r} is missing for "
                f"{command!r}"
            )


def _validate_action_provenance(
    action: RecoveryAction,
    schema: Mapping[str, Any],
) -> None:
    inputs = schema.get("inputs")
    if not isinstance(inputs, list):
        raise RecoveryContractError(
            f"Live action spec inputs are invalid for {action.command!r}"
        )

    defaults: dict[str, Any] = {}
    for raw_input in inputs:
        if not isinstance(raw_input, Mapping):
            raise RecoveryContractError(
                f"Live action spec input is invalid for {action.command!r}"
            )
        default = _action_input_default(raw_input)
        if default is not _MISSING:
            name = raw_input.get("name")
            if not isinstance(name, str) or not name:
                raise RecoveryContractError(
                    f"Live action spec input name is invalid for "
                    f"{action.command!r}"
                )
            defaults[name] = default

    explicit = action.explicit_arguments
    if explicit is not None and set(explicit) != set(action.arguments):
        raise RecoveryContractError(
            f"Recorded explicit arguments are invalid for {action.command!r}"
        )

    effective = action.arguments_effective
    if effective is None:
        if defaults:
            raise RecoveryContractError(
                f"Legacy recovery for {action.command!r} did not record the "
                "action defaults that were used"
            )
        return

    expected_effective = copy.deepcopy(defaults)
    expected_effective.update(copy.deepcopy(dict(action.arguments)))
    if not _json_values_equal(effective, expected_effective):
        raise RecoveryContractError(
            f"Live action defaults changed for {action.command!r}"
        )


def _action_input_default(input_schema: Mapping[str, Any]) -> Any:
    raw = input_schema.get("placeholder")
    if raw is None or raw == "":
        return _MISSING
    if not isinstance(raw, str):
        raise RecoveryContractError("Live action default is not text")

    input_type = input_schema.get("type")
    raw_schema = input_schema.get("schema")
    schema_type = (
        raw_schema.get("type")
        if isinstance(raw_schema, Mapping)
        else None
    )
    if input_type == "boolean":
        return raw == "true"
    if input_type == "int":
        try:
            return int(raw, 10)
        except ValueError:
            return _MISSING
    if input_type == "float":
        try:
            value = float(raw)
        except ValueError:
            return _MISSING
        return value if math.isfinite(value) else _MISSING
    if input_type == "array" or schema_type == "array":
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return raw
    return raw


def _path_value(
    value: Mapping[str, Any],
    path: tuple[str, ...],
) -> Any:
    current: Any = value
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _validate_input_value(
    value: Any,
    input_schema: Mapping[str, Any],
    field_name: str,
) -> None:
    if value is None:
        if input_schema.get("required") is True:
            raise RecoveryContractError(f"{field_name} cannot be null")
        return
    raw_schema = input_schema.get("schema")
    if raw_schema is not None and not isinstance(raw_schema, Mapping):
        raise RecoveryContractError(f"{field_name} has an invalid live schema")
    value_type = (
        raw_schema.get("type")
        if isinstance(raw_schema, Mapping) and raw_schema.get("type")
        else input_schema.get("type")
    )
    if not isinstance(value_type, str):
        raise RecoveryContractError(f"{field_name} has no live type")
    _validate_schema_value(
        value,
        value_type,
        raw_schema or input_schema,
        field_name,
    )
    enum = input_schema.get("enum")
    if enum is not None:
        if (
            not isinstance(enum, list)
            or not enum
            or not all(isinstance(item, str) for item in enum)
        ):
            raise RecoveryContractError(
                f"{field_name} has an invalid live enum"
            )
        if value not in enum:
            raise RecoveryContractError(
                f"{field_name} is outside the live enum"
            )


def _validate_schema_value(
    value: Any,
    value_type: str,
    schema: Mapping[str, Any],
    field_name: str,
) -> None:
    normalized = value_type.casefold()
    if normalized in ("int", "integer"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise RecoveryContractError(f"{field_name} must be an integer")
        _validate_number_range(value, schema, field_name)
        return
    if normalized in ("float", "number"):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise RecoveryContractError(f"{field_name} must be a number")
        _validate_number_range(value, schema, field_name)
        return
    if normalized == "boolean":
        if not isinstance(value, bool):
            raise RecoveryContractError(f"{field_name} must be a boolean")
        return
    if normalized in ("string", "enum"):
        if not isinstance(value, str):
            raise RecoveryContractError(f"{field_name} must be a string")
        if (
            schema.get("minLength") is not None
            and len(value)
            < _schema_nonnegative_int(schema, "minLength", field_name)
        ):
            raise RecoveryContractError(f"{field_name} is too short")
        if (
            schema.get("maxLength") is not None
            and len(value)
            > _schema_nonnegative_int(schema, "maxLength", field_name)
        ):
            raise RecoveryContractError(f"{field_name} is too long")
        return
    if normalized == "array":
        if not isinstance(value, list):
            raise RecoveryContractError(f"{field_name} must be an array")
        if (
            schema.get("minItems") is not None
            and len(value)
            < _schema_nonnegative_int(schema, "minItems", field_name)
        ):
            raise RecoveryContractError(f"{field_name} has too few items")
        if (
            schema.get("maxItems") is not None
            and len(value)
            > _schema_nonnegative_int(schema, "maxItems", field_name)
        ):
            raise RecoveryContractError(f"{field_name} has too many items")
        items = schema.get("items")
        if not isinstance(items, Mapping):
            raise RecoveryContractError(
                f"{field_name} has no safe live item schema"
            )
        item_type = items.get("type")
        if not isinstance(item_type, str):
            raise RecoveryContractError(
                f"{field_name} has an invalid live item type"
            )
        for index, item in enumerate(value):
            _validate_schema_value(
                item,
                item_type,
                items,
                f"{field_name}[{index}]",
            )
        return
    if normalized == "object":
        if not isinstance(value, Mapping):
            raise RecoveryContractError(f"{field_name} must be an object")
        _validate_object_schema(value, schema, field_name)
        return
    raise RecoveryContractError(
        f"{field_name} uses unsupported live type {value_type!r}"
    )


def _validate_object_schema(
    value: Mapping[str, Any],
    schema: Mapping[str, Any],
    field_name: str,
) -> None:
    properties = schema.get("properties")
    additional = schema.get("additionalProperties", False)
    if properties is None:
        properties = {}
    if not isinstance(properties, Mapping):
        raise RecoveryContractError(
            f"{field_name} has invalid live object properties"
        )
    required = schema.get("required", [])
    if (
        not isinstance(required, list)
        or not all(isinstance(item, str) for item in required)
    ):
        raise RecoveryContractError(
            f"{field_name} has invalid live required properties"
        )
    for name in required:
        if name not in value:
            raise RecoveryContractError(
                f"{field_name}.{name} is required"
            )
    for name, item in value.items():
        property_schema = properties.get(name)
        if property_schema is None:
            if additional is True or additional == {}:
                _validate_json(item)
                continue
            if isinstance(additional, Mapping):
                property_schema = additional
            else:
                raise RecoveryContractError(
                    f"{field_name}.{name} is not in the live schema"
                )
        if not isinstance(property_schema, Mapping):
            raise RecoveryContractError(
                f"{field_name}.{name} has an invalid live schema"
            )
        property_type = property_schema.get("type")
        if not isinstance(property_type, str):
            raise RecoveryContractError(
                f"{field_name}.{name} has no live type"
            )
        _validate_schema_value(
            item,
            property_type,
            property_schema,
            f"{field_name}.{name}",
        )


def _validate_number_range(
    value: int | float,
    schema: Mapping[str, Any],
    field_name: str,
) -> None:
    for key, comparison, message in (
        ("minimum", lambda left, right: left >= right, "below minimum"),
        ("maximum", lambda left, right: left <= right, "above maximum"),
        (
            "exclusiveMinimum",
            lambda left, right: left > right,
            "at or below exclusive minimum",
        ),
        (
            "exclusiveMaximum",
            lambda left, right: left < right,
            "at or above exclusive maximum",
        ),
    ):
        limit = schema.get(key)
        if limit is None:
            continue
        if (
            isinstance(limit, bool)
            or not isinstance(limit, (int, float))
            or not math.isfinite(float(limit))
        ):
            raise RecoveryContractError(
                f"{field_name} has an invalid live {key}"
            )
        if not comparison(value, limit):
            raise RecoveryContractError(f"{field_name} is {message}")


def _schema_nonnegative_int(
    schema: Mapping[str, Any],
    key: str,
    field_name: str,
) -> int:
    value = schema.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
    ):
        raise RecoveryContractError(
            f"{field_name} has an invalid live {key}"
        )
    return value


def build_guarded_resume_arguments(
    pointer: FlowPointer,
    *,
    rewind_steps: int,
    mode: str | None = None,
) -> Mapping[str, Any]:
    if pointer.action_index is None or pointer.action_command is None:
        raise RecoveryContractError(
            "cannot build continuation from a terminal Flow pointer"
        )
    if not 0 <= rewind_steps <= _MAX_REWIND_STEPS:
        raise RecoveryContractError("rewind_steps is outside the safe range")
    selected_mode = mode or (
        "retry_current" if rewind_steps == 0 else "rewind"
    )
    if selected_mode not in ("retry_current", "rewind"):
        raise RecoveryContractError("resume mode is invalid")
    if selected_mode == "retry_current" and rewind_steps != 0:
        raise RecoveryContractError("retry_current requires rewind_steps=0")
    arguments: dict[str, Any] = {
        "mode": selected_mode,
        "rewind_steps": rewind_steps,
        "current_action_index": pointer.action_index,
        "current_command": pointer.action_command,
        "flow_id": pointer.flow_id,
    }
    if pointer.filename is not None:
        arguments["filename"] = pointer.filename
    if pointer.fid is not None:
        arguments["fid"] = pointer.fid
    if pointer.flow_commit is not None or "flow_commit" in pointer.raw:
        arguments["flow_commit"] = pointer.flow_commit
    return arguments


def resume_arguments_for_plan(
    plan: RecoveryPlan,
    pointer: FlowPointer,
) -> Mapping[str, Any]:
    """Return the one terminal command payload for this pinned plan.

    A modern ``$resume_flow`` action is executed unchanged after its captured
    guard fields are proven equal to the fresh live pointer. Legacy ``$rerun``
    had no correlated guard, so it is the sole case upgraded to a fresh
    ``retry_current`` payload.
    """

    if plan.terminal_command == "$rerun":
        return build_guarded_resume_arguments(
            pointer,
            rewind_steps=0,
            mode="retry_current",
        )
    if plan.terminal_command != "$resume_flow":
        raise RecoveryContractError("recovery terminal command is invalid")
    arguments = copy.deepcopy(dict(plan.terminal_arguments))
    expected = {
        "current_action_index": pointer.action_index,
        "current_command": pointer.action_command,
    }
    if any(arguments.get(key) != value for key, value in expected.items()):
        raise RecoveryContractError(
            "Pinned $resume_flow step guard differs from the fresh Flow"
        )
    for key, live_value in (
        ("flow_id", pointer.flow_id),
        ("filename", pointer.filename),
        ("fid", pointer.fid or pointer.filename),
        ("flow_commit", pointer.flow_commit),
    ):
        if key in arguments and arguments.get(key) != live_value:
            raise RecoveryContractError(
                f"Pinned $resume_flow {key} differs from the fresh Flow"
            )
    return arguments


def validate_resume_acknowledgement(
    value: Any,
    *,
    pointer: FlowPointer,
    rewind_steps: int,
    mode: str | None = None,
) -> None:
    if not isinstance(value, Mapping):
        raise RecoveryContractError("$resume_flow acknowledgement is invalid")
    expected_mode = mode or (
        "retry_current" if rewind_steps == 0 else "rewind"
    )
    expected_target = pointer.action_index - rewind_steps
    required = {
        "accepted": True,
        "mode": expected_mode,
        "rewind_steps": rewind_steps,
        "previous_action_index": pointer.action_index,
        "target_action_index": expected_target,
        "current_command": pointer.action_command,
        "flow_id": pointer.flow_id,
    }
    if any(value.get(key) != expected for key, expected in required.items()):
        raise RecoveryContractError("$resume_flow acknowledgement does not match")
    if pointer.filename is not None and value.get("filename") != pointer.filename:
        raise RecoveryContractError("$resume_flow filename acknowledgement differs")
    if (
        pointer.flow_commit is not None
        and value.get("flow_commit") != pointer.flow_commit
    ):
        raise RecoveryContractError(
            "$resume_flow flow_commit acknowledgement differs"
        )


def classify_flow_outcome(
    pointer: FlowPointer | None,
    session: RecoverySessionRecord,
) -> str:
    if pointer is None or not _same_flow(pointer, session):
        return "unknown"
    status = pointer.status.strip().casefold()
    if pointer.completed and status in _SUCCESS_FLOW_STATUSES:
        return "recovered"
    if (
        status == "paused"
        and pointer.action_index == session.action_index
        and pointer.action_command == session.action_command
    ):
        return "failed"
    if (
        pointer.action_index is not None
        and pointer.action_index > session.action_index
    ):
        return "recovered"
    if (
        status in _SUCCESS_FLOW_STATUSES
        and pointer.action_index is not None
        and pointer.action_index >= session.action_index
    ):
        return "recovered"
    return "unknown"


def _execution_outcome_predicate(
    session: RecoverySessionRecord,
    *,
    generation: int,
) -> Callable[[FlowPointer | None], bool]:
    execution_started = False

    def decisive(pointer: FlowPointer | None) -> bool:
        nonlocal execution_started
        if (
            pointer is None
            or pointer.generation != generation
            or not _same_flow(pointer, session)
        ):
            return True
        status = pointer.status.strip().casefold()
        if pointer.completed and status in _SUCCESS_FLOW_STATUSES:
            return True
        if (
            pointer.action_index is not None
            and pointer.action_index > session.action_index
        ):
            return True
        if status in _SUCCESS_FLOW_STATUSES:
            return True
        if not execution_started:
            if status == "paused":
                # Brain publishes the jump target before it begins execution.
                return False
            if status == "in_progress":
                execution_started = True
                return False
            return True
        if status == "paused":
            return True
        if status in ("aborted", "error"):
            return True
        return False

    return decisive


def _is_decisive_flow_outcome(
    pointer: FlowPointer | None,
    session: RecoverySessionRecord,
) -> bool:
    if pointer is None or not _same_flow(pointer, session):
        return True
    status = pointer.status.strip().casefold()
    if pointer.completed and status in _SUCCESS_FLOW_STATUSES:
        return True
    if status == "paused":
        return True
    if (
        pointer.action_index is not None
        and pointer.action_index > session.action_index
    ):
        return True
    return (
        status in _SUCCESS_FLOW_STATUSES
        and pointer.action_index is not None
        and pointer.action_index >= session.action_index
    )


def _pointer_matches_target(
    pointer: FlowPointer | None,
    target: FailureRecoveryTarget,
) -> bool:
    return bool(
        pointer is not None
        and pointer.status.strip().casefold() == "paused"
        and pointer.flow_id == target.flow_id
        and pointer.action_index == target.action_index
        and pointer.action_command == target.action_command
    )


def _pointer_matches_session(
    pointer: FlowPointer,
    session: RecoverySessionRecord,
) -> bool:
    return bool(
        pointer.status.strip().casefold() == "paused"
        and pointer.flow_id == session.flow_id
        and pointer.action_index == session.action_index
        and pointer.action_command == session.action_command
    )


def _same_flow(
    pointer: FlowPointer,
    session: RecoverySessionRecord,
) -> bool:
    return pointer.flow_id == session.flow_id


def _json_values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        return (
            isinstance(left, (int, float))
            and not isinstance(left, bool)
            and isinstance(right, (int, float))
            and not isinstance(right, bool)
            and math.isfinite(float(left))
            and math.isfinite(float(right))
            and float(left) == float(right)
        )
    if left is None or right is None or isinstance(left, str) or isinstance(right, str):
        return type(left) is type(right) and left == right
    if isinstance(left, list) or isinstance(right, list):
        return (
            isinstance(left, list)
            and isinstance(right, list)
            and len(left) == len(right)
            and all(
                _json_values_equal(left_item, right_item)
                for left_item, right_item in zip(left, right)
            )
        )
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        return (
            isinstance(left, Mapping)
            and isinstance(right, Mapping)
            and set(left) == set(right)
            and all(
                _json_values_equal(left[key], right[key])
                for key in left
            )
        )
    return False


def _validate_json(value: Any) -> None:
    nodes = 0

    def walk(current: Any, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_JSON_NODES or depth > _MAX_JSON_DEPTH:
            raise RecoveryContractError("JSON recovery arguments are too complex")
        if current is None or isinstance(current, (bool, str)):
            return
        if isinstance(current, (int, float)) and not isinstance(current, bool):
            if isinstance(current, float) and not math.isfinite(current):
                raise RecoveryContractError(
                    "JSON recovery arguments contain a non-finite number"
                )
            return
        if isinstance(current, list):
            for item in current:
                walk(item, depth + 1)
            return
        if isinstance(current, Mapping):
            for key, item in current.items():
                if not isinstance(key, str):
                    raise RecoveryContractError(
                        "JSON recovery argument keys must be strings"
                    )
                walk(item, depth + 1)
            return
        raise RecoveryContractError(
            "Recovery arguments contain a non-JSON value"
        )

    walk(value, 0)


def _action_index(value: Mapping[str, Any], field_name: str) -> int:
    for key in ("actionIndex", "action_index", "index"):
        if key in value:
            return _required_int(
                value.get(key),
                f"{field_name}.{key}",
                minimum=0,
                maximum=1_000_000,
            )
    raise RecoveryContractError(f"{field_name} has no action index")


def _action_command(value: Mapping[str, Any], field_name: str) -> str:
    return _required_text(
        value.get("command"),
        f"{field_name}.command",
        max_length=_MAX_COMMAND_LENGTH,
    )


def _parse_continuation_action(
    value: Any,
    field_name: str,
) -> FlowActionContext:
    if not isinstance(value, Mapping):
        raise RecoveryContractError(f"{field_name} must be an object")
    if set(value) - {
        "command",
        "description",
        "status",
        "areaName",
        "itemName",
        "actionIndex",
    }:
        raise RecoveryContractError(f"{field_name} has unsupported fields")
    action_index = _required_int(
        value.get("actionIndex"),
        f"{field_name}.actionIndex",
        minimum=0,
        maximum=1_000_000,
    )
    command = _required_text(
        value.get("command"),
        f"{field_name}.command",
        max_length=_MAX_COMMAND_LENGTH,
    )
    area_name = _required_text(
        value.get("areaName"),
        f"{field_name}.areaName",
        max_length=300,
    )
    item_name = _required_text(
        value.get("itemName"),
        f"{field_name}.itemName",
        max_length=300,
    )
    for optional in ("description", "status"):
        if optional in value:
            _required_text(
                value.get(optional),
                f"{field_name}.{optional}",
                max_length=1_000,
            )
    return FlowActionContext(
        action_index=action_index,
        command=command,
        area_name=area_name,
        item_name=item_name,
    )


def _required_text(value: Any, name: str, *, max_length: int) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise RecoveryContractError(f"{name} must be non-empty trimmed text")
    if len(value) > max_length:
        raise RecoveryContractError(f"{name} is too long")
    return value


def _required_int(
    value: Any,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RecoveryContractError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise RecoveryContractError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


def _required_uuid(value: Any, name: str) -> str:
    parsed = _uuid_text(value)
    if parsed is None:
        raise RecoveryContractError(f"{name} must be a UUID")
    return parsed


def _uuid_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    canonical = value.strip().lower()
    if not _UUID_PATTERN.fullmatch(canonical):
        return None
    try:
        return str(uuid.UUID(canonical))
    except ValueError:
        return None


def _response_rows(response: Any) -> list[Mapping[str, Any]]:
    data = getattr(response, "data", None)
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, Mapping)]


async def _wait_for_stop(
    stop_event: asyncio.Event,
    seconds: float,
) -> bool:
    try:
        await asyncio.wait_for(
            stop_event.wait(),
            timeout=max(0.001, seconds),
        )
        return True
    except TimeoutError:
        return False
