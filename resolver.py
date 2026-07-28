"""Online failure resolver backed by Supabase events and Markdown Git memory.

Supabase is the event and outcome source.  It is deliberately not a memory
store: successful operator-demonstrated resolutions are generalized into
Markdown and committed through :mod:`memory_store`.  At match time the model
may select one existing Markdown memory ID or no solution.  Executable actions
are copied verbatim from the selected document; model output is never treated
as a command.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Mapping, Protocol, Sequence

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from agent import (
    AgentReasoningError,
    GeneralizationResult,
    MarkdownMemory,
    MemoryChoiceResult,
    OpenAIFailureAgent,
)
from memory_store import (
    GitMemoryConfig,
    GitMemoryStore,
    MemoryDocument,
    MemoryDraft,
    MemoryWriteResult,
    ResolutionSource,
    resolution_source_from_row,
)
from observer import (
    ChangeSignal,
    ClientFactory,
    ObserverSettings,
    SupabaseFailureObserver,
    _configure_logging,
    create_supabase_client,
)


logger = logging.getLogger("failure_resolver.agent")

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESOLUTION_EVENTS = ("INSERT", "UPDATE")
_FAILURE_ID = "failure_id"
_RESOLUTION_ID = "resolution_id"
_MAX_MATCH_CANDIDATES = 50
_MAX_MATCHER_MESSAGE_LENGTH = 800
_MAX_SUGGESTION_TEXT_LENGTH = 2_000
_LOCATION_FIELDS = (
    "site_id",
    "site",
    "floor",
    "room_number",
    "map_id",
    "map_name",
    "map_observed_at",
)


class FailureAgent(Protocol):
    async def choose_memory(
        self,
        failure: Mapping[str, Any],
        memories: Sequence[MarkdownMemory],
    ) -> MemoryChoiceResult: ...

    async def generalize_resolution(
        self,
        resolution: Mapping[str, Any],
    ) -> GeneralizationResult: ...

    async def close(self) -> None: ...


class MarkdownMemoryStore(Protocol):
    async def arebuild_index(
        self,
        *,
        refresh: bool = True,
    ) -> dict[str, MemoryDocument]: ...

    async def ahas_source_hash(
        self,
        resolution_id: str,
        source_hash: str,
        *,
        refresh: bool = True,
    ) -> bool: ...

    async def awrite_memory(self, draft: MemoryDraft) -> MemoryWriteResult: ...


@dataclass(frozen=True)
class ResolverSettings:
    supabase_url: str
    supabase_service_role_key: str = field(repr=False)
    openai_api_key: str = field(repr=False)
    openai_model: str
    memory_repo_url: str = field(repr=False)
    memory_repo_root: Path
    memory_repo_branch: str = "main"
    failure_events_table: str = "failure_events"
    resolutions_table: str = "flow_failure_resolutions"
    schema: str = "public"
    reconcile_limit: int = 500
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    subscription_timeout_seconds: float = 20.0
    connection_check_seconds: float = 1.0
    disconnect_grace_seconds: float = 10.0
    shutdown_timeout_seconds: float = 15.0

    @classmethod
    def from_env(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "ResolverSettings":
        environment = environment or os.environ
        mode = environment.get("RESOLVER_MODE", "agent").strip().lower()
        if mode != "agent":
            raise ValueError("The resolver entrypoint requires RESOLVER_MODE=agent")

        url = environment.get("SUPABASE_URL", "").strip().rstrip("/")
        supabase_key = environment.get(
            "SUPABASE_SERVICE_ROLE_KEY",
            "",
        ).strip()
        openai_key = environment.get("OPENAI_API_KEY", "").strip()
        model = environment.get("OPENAI_MODEL", "gpt-5.6-luna").strip()
        repo_url = environment.get("MEMORY_REPO_URL", "").strip()
        repo_root = environment.get(
            "MEMORY_REPO_ROOT",
            "/var/lib/failure-resolver/memory",
        ).strip()
        branch = environment.get("MEMORY_REPO_BRANCH", "main").strip()
        failure_table = environment.get(
            "FAILURE_EVENTS_TABLE",
            "failure_events",
        ).strip()
        resolutions_table = environment.get(
            "FLOW_FAILURE_RESOLUTIONS_TABLE",
            "flow_failure_resolutions",
        ).strip()
        schema = environment.get("SUPABASE_SCHEMA", "public").strip()

        if not url.startswith(("https://", "http://")):
            raise ValueError("SUPABASE_URL must be an http(s) URL")
        if not supabase_key:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY is required")
        if not openai_key:
            raise ValueError("OPENAI_API_KEY is required")
        if not model:
            raise ValueError("OPENAI_MODEL is required")
        if not repo_url:
            raise ValueError("MEMORY_REPO_URL is required")
        if not repo_root:
            raise ValueError("MEMORY_REPO_ROOT is required")
        for variable, value in (
            ("FAILURE_EVENTS_TABLE", failure_table),
            ("FLOW_FAILURE_RESOLUTIONS_TABLE", resolutions_table),
            ("SUPABASE_SCHEMA", schema),
        ):
            if not _VALID_IDENTIFIER.fullmatch(value):
                raise ValueError(f"{variable} must be a PostgreSQL identifier")

        return cls(
            supabase_url=url,
            supabase_service_role_key=supabase_key,
            openai_api_key=openai_key,
            openai_model=model,
            memory_repo_url=repo_url,
            memory_repo_root=Path(repo_root),
            memory_repo_branch=branch,
            failure_events_table=failure_table,
            resolutions_table=resolutions_table,
            schema=schema,
            reconcile_limit=_positive_int(
                environment,
                "RESOLVER_RECONCILE_LIMIT",
                500,
            ),
            reconnect_initial_seconds=_positive_float(
                environment,
                "OBSERVER_RECONNECT_INITIAL_SECONDS",
                1.0,
            ),
            reconnect_max_seconds=_positive_float(
                environment,
                "OBSERVER_RECONNECT_MAX_SECONDS",
                30.0,
            ),
            subscription_timeout_seconds=_positive_float(
                environment,
                "OBSERVER_SUBSCRIPTION_TIMEOUT_SECONDS",
                20.0,
            ),
            connection_check_seconds=_positive_float(
                environment,
                "OBSERVER_CONNECTION_CHECK_SECONDS",
                1.0,
            ),
            disconnect_grace_seconds=_positive_float(
                environment,
                "OBSERVER_DISCONNECT_GRACE_SECONDS",
                10.0,
            ),
            shutdown_timeout_seconds=_positive_float(
                environment,
                "OBSERVER_SHUTDOWN_TIMEOUT_SECONDS",
                15.0,
            ),
        )

    def observer_settings(self) -> ObserverSettings:
        return ObserverSettings(
            supabase_url=self.supabase_url,
            supabase_service_role_key=self.supabase_service_role_key,
            failure_events_table=self.failure_events_table,
            schema=self.schema,
            reconnect_initial_seconds=self.reconnect_initial_seconds,
            reconnect_max_seconds=self.reconnect_max_seconds,
            subscription_timeout_seconds=self.subscription_timeout_seconds,
            connection_check_seconds=self.connection_check_seconds,
            disconnect_grace_seconds=self.disconnect_grace_seconds,
            shutdown_timeout_seconds=self.shutdown_timeout_seconds,
        )


@dataclass
class ResolverState:
    failures_claimed: int = 0
    solutions_found: int = 0
    no_solution: int = 0
    failures_skipped: int = 0
    match_errors: int = 0
    resolutions_observed: int = 0
    resolutions_skipped: int = 0
    memories_written: int = 0
    memory_errors: int = 0
    memory_acks_ingested: int = 0
    memory_acks_failed: int = 0
    last_failure_id: str | None = None
    last_resolution_id: str | None = None
    last_memory_commit: str | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "failures_claimed": self.failures_claimed,
            "solutions_found": self.solutions_found,
            "no_solution": self.no_solution,
            "failures_skipped": self.failures_skipped,
            "match_errors": self.match_errors,
            "resolutions_observed": self.resolutions_observed,
            "resolutions_skipped": self.resolutions_skipped,
            "memories_written": self.memories_written,
            "memory_errors": self.memory_errors,
            "memory_acks_ingested": self.memory_acks_ingested,
            "memory_acks_failed": self.memory_acks_failed,
            "last_failure_id": self.last_failure_id,
            "last_resolution_id": self.last_resolution_id,
            "last_memory_commit": self.last_memory_commit,
        }


@dataclass(frozen=True)
class FailureMatch:
    failure_id: str
    memory_id: str
    suggested_fix: str
    confidence: float
    reason: str
    actions: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class ResolutionChangeSignal:
    event: str | None
    resolution_id: str
    commit_timestamp: str | None


class FailureResolverProcessor:
    """Turn database rows into match decisions and Markdown memories."""

    def __init__(
        self,
        *,
        failure_events_table: str,
        resolutions_table: str,
        agent: FailureAgent,
        memory_store: MarkdownMemoryStore,
        client: Any | None = None,
    ) -> None:
        self.failure_events_table = failure_events_table
        self.resolutions_table = resolutions_table
        self.agent = agent
        self.memory_store = memory_store
        self.client = client
        self.state = ResolverState()
        self._known_commit_shas: dict[str, str] = {}

    async def process_failure(self, failure_id: str) -> FailureMatch | None:
        row = await self._fetch_row(
            self.failure_events_table,
            _FAILURE_ID,
            failure_id,
        )
        if row is None:
            self.state.failures_skipped += 1
            return None
        if _normalized_text(row.get("matcher_status")) != "pending":
            self.state.failures_skipped += 1
            return None
        if not await self._claim_failure(failure_id):
            self.state.failures_skipped += 1
            return None

        self.state.failures_claimed += 1
        self.state.last_failure_id = failure_id
        try:
            index = await self.memory_store.arebuild_index(refresh=True)
            candidates = _ranked_execution_candidates(row, index.values())
            if not candidates:
                await self._finish_failure(
                    failure_id,
                    "no_solution",
                    "No applicable recovery memory is available.",
                )
                self.state.no_solution += 1
                return None

            choice_result = await self.agent.choose_memory(
                _failure_with_location_context(row),
                [
                    MarkdownMemory(
                        memory_id=document.resolution_id,
                        markdown=_memory_retrieval_text(document),
                    )
                    for document in candidates
                ],
            )
            choice = choice_result.choice
            if choice.decision == "no_solution":
                await self._finish_failure(
                    failure_id,
                    "no_solution",
                    f"No applicable solution: {choice.reason}",
                )
                self.state.no_solution += 1
                return None

            selected = {
                document.resolution_id: document
                for document in candidates
            }.get(choice.memory_id or "")
            if selected is None or not _is_safe_execution_candidate(selected):
                raise AgentReasoningError(
                    "Selected memory is not an execution candidate"
                )

            # Deep-copy the immutable source document.  Nothing from the model
            # can enter this executable payload.
            actions = tuple(
                _suggestion_action(action)
                for action in selected.dispatched_actions
            )
            if not actions:
                raise AgentReasoningError(
                    "Selected memory contains no dispatched actions"
                )
            suggested_fix = _suggested_fix_text(selected)
            match = FailureMatch(
                failure_id=failure_id,
                memory_id=selected.resolution_id,
                suggested_fix=suggested_fix,
                confidence=choice.confidence,
                reason=choice.reason,
                actions=actions,
            )
            await self._finish_failure(
                failure_id,
                "solution_found",
                f"Suggested fix: {suggested_fix}",
                suggestion={
                    "memory_id": match.memory_id,
                    "summary": match.suggested_fix,
                    "reason": match.reason,
                    "confidence": match.confidence,
                    "actions": list(match.actions),
                },
            )
            self.state.solutions_found += 1
            return match
        except asyncio.CancelledError:
            raise
        except Exception:
            self.state.match_errors += 1
            await self._finish_failure(
                failure_id,
                "failed",
                "The solution matcher could not complete safely.",
            )
            raise

    async def learn_resolution(
        self,
        resolution_id: str,
    ) -> MemoryWriteResult | None:
        row = await self._fetch_row(
            self.resolutions_table,
            _RESOLUTION_ID,
            resolution_id,
        )
        self.state.resolutions_observed += 1
        self.state.last_resolution_id = resolution_id
        if row is None or not _is_positive_demonstration(row):
            self.state.resolutions_skipped += 1
            return None

        failure_id = _safe_identifier(row.get(_FAILURE_ID))
        acknowledgement = await self._claim_memory_ingestion(
            failure_id,
            resolution_id,
        )
        if acknowledgement == "owned_elsewhere":
            self.state.resolutions_skipped += 1
            return None

        try:
            memory_row = _row_with_successful_actions(row)
            source = resolution_source_from_row(memory_row)
            if await self.memory_store.ahas_source_hash(
                source.resolution_id,
                source.source_hash,
                refresh=True,
            ):
                self.state.resolutions_skipped += 1
                if acknowledgement == "claimed":
                    await self._mark_memory_ingested(
                        failure_id,
                        source.resolution_id,
                        commit_sha=self._known_commit_shas.get(
                            source.resolution_id
                        ),
                    )
                return None

            generalized = await self.agent.generalize_resolution(memory_row)
            if tuple(source.dispatched_actions) != generalized.demonstrated_actions:
                raise AgentReasoningError(
                    "Generalized action evidence differs from the source"
                )
            draft = _memory_draft(source, generalized)
            result = await self.memory_store.awrite_memory(draft)
            if result.changed:
                self.state.memories_written += 1
                self.state.last_memory_commit = result.commit_sha
                if result.commit_sha:
                    self._known_commit_shas[source.resolution_id] = (
                        result.commit_sha
                    )
            else:
                self.state.resolutions_skipped += 1
            if acknowledgement == "claimed":
                await self._mark_memory_ingested(
                    failure_id,
                    source.resolution_id,
                    commit_sha=result.commit_sha,
                )
            return result
        except asyncio.CancelledError:
            raise
        except Exception:
            self.state.memory_errors += 1
            if acknowledgement == "claimed":
                try:
                    await self._mark_memory_failed(
                        failure_id,
                        resolution_id,
                    )
                except Exception as error:
                    logger.warning(
                        "Memory acknowledgement failed error_type=%s",
                        type(error).__name__,
                    )
            raise

    async def pending_memory_resolution(
        self,
        failure_id: str,
    ) -> str | None:
        row = await self._fetch_row(
            self.failure_events_table,
            _FAILURE_ID,
            failure_id,
        )
        if row is None or _normalized_text(row.get("memory_status")) != "pending":
            return None
        return _safe_identifier(row.get("memory_resolution_id"))

    async def close(self) -> None:
        await self.agent.close()

    async def _fetch_row(
        self,
        table: str,
        id_column: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        client = self._required_client()
        response = await (
            client.table(table)
            .select("*")
            .eq(id_column, record_id)
            .limit(1)
            .execute()
        )
        data = getattr(response, "data", None)
        if not isinstance(data, list) or not data:
            return None
        row = data[0]
        return dict(row) if isinstance(row, Mapping) else None

    async def _claim_failure(self, failure_id: str) -> bool:
        client = self._required_client()
        response = await (
            client.table(self.failure_events_table)
            .update(
                {
                    "matcher_status": "matching",
                    "matcher_message": "Checking solution memory.",
                    "resolver_suggestion": None,
                }
            )
            .eq(_FAILURE_ID, failure_id)
            .eq("matcher_status", "pending")
            .execute()
        )
        data = getattr(response, "data", None)
        return isinstance(data, list) and bool(data)

    async def _claim_memory_ingestion(
        self,
        failure_id: str | None,
        resolution_id: str,
    ) -> Literal["claimed", "owned_elsewhere", "untracked"]:
        if failure_id is None:
            return "untracked"
        current = await self._fetch_row(
            self.failure_events_table,
            _FAILURE_ID,
            failure_id,
        )
        if current is None:
            return "untracked"
        current_resolution = _safe_identifier(
            current.get("memory_resolution_id")
        )
        current_status = _normalized_text(current.get("memory_status"))
        claimable_statuses = {"pending", "failed"}
        if (
            current_resolution != resolution_id
            or current_status not in claimable_statuses
        ):
            if (
                current_resolution == resolution_id
                and current_status in {"ingesting", "ingested"}
            ):
                return "owned_elsewhere"
            return "untracked"

        client = self._required_client()
        response = await (
            client.table(self.failure_events_table)
            .update(
                {
                    "memory_status": "ingesting",
                    "memory_resolution_id": resolution_id,
                    "memory_commit_sha": None,
                    "memory_message": "Ingesting recovery memory.",
                    "memory_ingested_at": None,
                }
            )
            .eq(_FAILURE_ID, failure_id)
            # The status read above is part of the compare-and-set. A later
            # resolution event or cold-start reconciliation can therefore
            # retry a failed ingestion without racing another worker.
            .eq("memory_status", current_status)
            .eq("memory_resolution_id", resolution_id)
            .execute()
        )
        data = getattr(response, "data", None)
        return "claimed" if isinstance(data, list) and data else "owned_elsewhere"

    async def _mark_memory_ingested(
        self,
        failure_id: str | None,
        resolution_id: str,
        *,
        commit_sha: str | None,
    ) -> None:
        if failure_id is None:
            return
        client = self._required_client()
        await (
            client.table(self.failure_events_table)
            .update(
                {
                    "memory_status": "ingested",
                    "memory_resolution_id": resolution_id,
                    "memory_commit_sha": commit_sha,
                    "memory_message": "Recovery memory ingested.",
                    "memory_ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            .eq(_FAILURE_ID, failure_id)
            .eq("memory_status", "ingesting")
            .eq("memory_resolution_id", resolution_id)
            .execute()
        )
        self.state.memory_acks_ingested += 1

    async def _mark_memory_failed(
        self,
        failure_id: str | None,
        resolution_id: str,
    ) -> None:
        if failure_id is None:
            return
        client = self._required_client()
        await (
            client.table(self.failure_events_table)
            .update(
                {
                    "memory_status": "failed",
                    "memory_resolution_id": resolution_id,
                    "memory_commit_sha": None,
                    "memory_message": "Recovery memory ingestion failed.",
                    "memory_ingested_at": None,
                }
            )
            .eq(_FAILURE_ID, failure_id)
            .eq("memory_status", "ingesting")
            .eq("memory_resolution_id", resolution_id)
            .execute()
        )
        self.state.memory_acks_failed += 1

    async def _finish_failure(
        self,
        failure_id: str,
        status: str,
        message: str,
        *,
        suggestion: Mapping[str, Any] | None = None,
    ) -> None:
        client = self._required_client()
        await (
            client.table(self.failure_events_table)
            .update(
                {
                    "matcher_status": status,
                    "matcher_message": _bounded_matcher_message(message),
                    "resolver_suggestion": (
                        copy.deepcopy(dict(suggestion))
                        if suggestion is not None
                        else None
                    ),
                }
            )
            .eq(_FAILURE_ID, failure_id)
            .eq("matcher_status", "matching")
            .execute()
        )

    def _required_client(self) -> Any:
        if self.client is None:
            raise ConnectionError("Supabase client is unavailable")
        return self.client


class SupabaseAgentRuntime(SupabaseFailureObserver):
    """Realtime runtime reusing the observer's reconnect and health lifecycle."""

    def __init__(
        self,
        settings: ResolverSettings,
        *,
        agent: FailureAgent,
        memory_store: MarkdownMemoryStore,
        client_factory: ClientFactory = create_supabase_client,
    ) -> None:
        super().__init__(
            settings.observer_settings(),
            client_factory=client_factory,
        )
        self.resolver_settings = settings
        self.processor = FailureResolverProcessor(
            failure_events_table=settings.failure_events_table,
            resolutions_table=settings.resolutions_table,
            agent=agent,
            memory_store=memory_store,
        )
        self._resolution_channel: Any | None = None
        self._resolution_subscription_event = asyncio.Event()
        self._resolution_subscription_error: BaseException | None = None
        # The base observer serializes this queue through one worker, which
        # keeps Git writes and status transitions ordered.
        self._fetch_queue = asyncio.Queue(
            maxsize=max(256, settings.reconcile_limit * 2 + 32)
        )

    async def run(self) -> None:
        try:
            await super().run()
        finally:
            await self.processor.close()

    async def _connect(self) -> None:
        await super()._connect()
        self.processor.client = self._client
        await self._connect_resolution_channel()
        await self._enqueue_reconciliation()

    async def _connect_resolution_channel(self) -> None:
        client = self._client
        if client is None:
            raise ConnectionError("Supabase client is unavailable")
        self._resolution_subscription_event = asyncio.Event()
        self._resolution_subscription_error = None
        self._resolution_channel = client.channel(
            (
                f"failure-resolver:{self.resolver_settings.schema}:"
                f"{self.resolver_settings.resolutions_table}"
            )
        )
        for event in _RESOLUTION_EVENTS:
            self._resolution_channel.on_postgres_changes(
                event,
                schema=self.resolver_settings.schema,
                table=self.resolver_settings.resolutions_table,
                callback=self._on_resolution_change,
            )
        await self._resolution_channel.subscribe(
            self._on_resolution_subscription_status
        )
        await asyncio.wait_for(
            self._resolution_subscription_event.wait(),
            timeout=self.resolver_settings.subscription_timeout_seconds,
        )
        if self._resolution_subscription_error is not None:
            raise self._resolution_subscription_error

    def _on_resolution_subscription_status(
        self,
        status: Any,
        error: BaseException | None = None,
    ) -> None:
        name = _status_name(status)
        if name == "SUBSCRIBED":
            self._resolution_subscription_event.set()
            return
        self._resolution_subscription_error = error or ConnectionError(name)
        self._mark_disconnected(type(error).__name__ if error else name)
        self._resolution_subscription_event.set()
        self._reconnect_event.set()

    def _on_resolution_change(self, payload: Any) -> None:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        record = data.get("record", {}) if isinstance(data, dict) else {}
        resolution_id = (
            _safe_identifier(record.get(_RESOLUTION_ID))
            if isinstance(record, Mapping)
            else None
        )
        if resolution_id is None:
            self.state.events_dropped += 1
            return
        signal = ResolutionChangeSignal(
            event=_safe_identifier(data.get("type")),
            resolution_id=resolution_id,
            commit_timestamp=_safe_identifier(data.get("commit_timestamp")),
        )
        self._enqueue_signal(signal)

    def _enqueue_signal(
        self,
        signal: ChangeSignal | ResolutionChangeSignal,
    ) -> None:
        try:
            self._fetch_queue.put_nowait(signal)
            self.state.events_observed += 1
        except asyncio.QueueFull:
            self.state.events_dropped += 1
            logger.warning("Resolver work queue is full")

    async def _fetch_and_log_safe_row(
        self,
        signal: ChangeSignal | ResolutionChangeSignal,
    ) -> None:
        if isinstance(signal, ResolutionChangeSignal):
            await self.processor.learn_resolution(signal.resolution_id)
            self.state.rows_fetched += 1
            logger.info(
                "Processed resolution change resolution_id=%r event=%r",
                signal.resolution_id,
                signal.event,
            )
            return
        pending_resolution = await self.processor.pending_memory_resolution(
            signal.failure_id
        )
        if pending_resolution is not None:
            await self.processor.learn_resolution(pending_resolution)
        await self.processor.process_failure(signal.failure_id)
        self.state.rows_fetched += 1
        logger.info(
            "Processed failure change failure_id=%r event=%r",
            signal.failure_id,
            signal.event,
        )

    async def _enqueue_reconciliation(self) -> None:
        client = self._client
        if client is None:
            raise ConnectionError("Supabase client is unavailable")
        # Rebuild missing durable memories before matching pending failures on
        # cold start.  Git usually makes this a cheap source-hash no-op.
        resolutions = await (
            client.table(self.resolver_settings.resolutions_table)
            .select(_RESOLUTION_ID)
            .eq("outcome", "resolved")
            .eq("applied", True)
            .order("created_at")
            .limit(self.resolver_settings.reconcile_limit)
            .execute()
        )
        for row in _response_rows(resolutions):
            resolution_id = _safe_identifier(row.get(_RESOLUTION_ID))
            if resolution_id is not None:
                self._enqueue_signal(
                    ResolutionChangeSignal(
                        event="RECONCILE",
                        resolution_id=resolution_id,
                        commit_timestamp=None,
                    )
                )

        pending = await (
            client.table(self.resolver_settings.failure_events_table)
            .select(_FAILURE_ID)
            .eq("matcher_status", "pending")
            .order("created_at")
            .limit(self.resolver_settings.reconcile_limit)
            .execute()
        )
        for row in _response_rows(pending):
            failure_id = _safe_identifier(row.get(_FAILURE_ID))
            if failure_id is not None:
                self._enqueue_signal(
                    ChangeSignal(
                        event="RECONCILE",
                        failure_id=failure_id,
                        commit_timestamp=None,
                    )
                )

    def _connection_is_healthy(self) -> bool:
        if not super()._connection_is_healthy():
            return False
        return bool(
            self._resolution_channel is not None
            and getattr(self._resolution_channel, "is_joined", True)
        )

    async def _disconnect(self) -> None:
        client = self._client
        channel = self._resolution_channel
        self._resolution_channel = None
        self.processor.client = None
        if client is not None and channel is not None:
            try:
                await client.remove_channel(channel)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "Resolution subscription cleanup failed error_type=%s",
                    type(error).__name__,
                )
        await super()._disconnect()

    def snapshot(self) -> dict[str, Any]:
        snapshot = self.state.snapshot()
        snapshot["mode"] = "agent"
        snapshot.update(self.processor.state.snapshot())
        return snapshot


def _memory_draft(
    source: ResolutionSource,
    result: GeneralizationResult,
) -> MemoryDraft:
    generalization = result.generalization
    lessons = tuple(f"Retrieval tag: {tag}" for tag in generalization.tags)
    return MemoryDraft(
        source=source,
        memory_kind="positive",
        actionable=True,
        failure_summary=generalization.failure_pattern,
        recovery_summary=generalization.resolution_summary,
        lessons=lessons,
        model=result.model,
        response_id=result.response_id,
    )


def _is_positive_demonstration(row: Mapping[str, Any]) -> bool:
    return (
        _normalized_text(row.get("outcome")) == "resolved"
        and row.get("applied") is True
        and bool(_successful_action_runs(row))
    )


def _row_with_successful_actions(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["action_runs"] = _successful_action_runs(row)
    return normalized


def _successful_action_runs(
    row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    action_runs = row.get("action_runs")
    if not isinstance(action_runs, list):
        return []
    successful: list[dict[str, Any]] = []
    for action in action_runs:
        if not isinstance(action, Mapping):
            continue
        command = action.get("command")
        if (
            action.get("status") == "sent"
            and isinstance(command, str)
            and bool(command.strip())
            and _has_usable_rerun_context(action)
        ):
            successful.append(copy.deepcopy(dict(action)))
    return successful


def _failure_with_location_context(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    prepared = dict(row)
    existing = row.get("sanitized_context")
    context = copy.deepcopy(dict(existing)) if isinstance(existing, Mapping) else {}
    location: dict[str, Any] = {}
    existing_location = context.get("location")
    if isinstance(existing_location, Mapping):
        location.update(copy.deepcopy(dict(existing_location)))

    for field_name in _LOCATION_FIELDS:
        value = row.get(field_name)
        if value is not None:
            location[field_name] = copy.deepcopy(value)

    navigation = row.get("navigation")
    if not isinstance(navigation, Mapping):
        navigation = context.get("navigation")
    current_map = (
        navigation.get("current_map")
        if isinstance(navigation, Mapping)
        else None
    )
    if isinstance(current_map, Mapping):
        if "map_id" not in location and current_map.get("id") is not None:
            location["map_id"] = copy.deepcopy(current_map.get("id"))
        if "map_name" not in location and current_map.get("map_name") is not None:
            location["map_name"] = copy.deepcopy(current_map.get("map_name"))

    if location:
        context["location"] = location
    prepared["sanitized_context"] = context
    return prepared


def _ranked_execution_candidates(
    failure: Mapping[str, Any],
    documents: Iterable[MemoryDocument],
) -> tuple[MemoryDocument, ...]:
    candidates = [
        document
        for document in documents
        if _is_safe_execution_candidate(document)
    ]
    values = _failure_retrieval_values(failure)

    def score(document: MemoryDocument) -> tuple[int, str]:
        frontmatter = document.frontmatter
        weights = {
            "flow_id": 8,
            "failed_command": 7,
            "item_name": 6,
            "flow_name": 5,
            "area_name": 4,
            "site": 3,
            "room_number": 3,
            "sysid": 2,
        }
        total = 0
        for field_name, weight in weights.items():
            expected = _normalized_text(values.get(field_name))
            actual = _normalized_text(frontmatter.get(field_name))
            if expected and actual and expected == actual:
                total += weight
        return (-total, document.resolution_id)

    return tuple(sorted(candidates, key=score)[:_MAX_MATCH_CANDIDATES])


def _is_safe_execution_candidate(document: MemoryDocument) -> bool:
    if not document.is_execution_candidate:
        return False
    for action in document.dispatched_actions:
        command = action.get("command")
        if (
            action.get("status") != "sent"
            or not isinstance(command, str)
            or not command.strip()
        ):
            return False
        if not _has_usable_rerun_context(action):
            return False
    return True


def _has_usable_rerun_context(action: Mapping[str, Any]) -> bool:
    command = action.get("command")
    if not isinstance(command, str) or command.strip() != "$rerun":
        return True
    retry_context = action.get("retry_context")
    retried_action = (
        retry_context.get("retried_action")
        if isinstance(retry_context, Mapping)
        else None
    )
    if not isinstance(retried_action, Mapping) or not retried_action:
        return False
    retried_command = retried_action.get("command")
    return isinstance(retried_command, str) and bool(retried_command.strip())


def _suggested_fix_text(document: MemoryDocument) -> str:
    marker = "\n## Recovery Knowledge\n"
    if marker in document.body:
        section = document.body.split(marker, 1)[1].split("\n## ", 1)[0]
        unquoted: list[str] = []
        for line in section.strip().splitlines():
            if line == ">":
                unquoted.append("")
            elif line.startswith("> "):
                unquoted.append(line[2:])
            elif line.strip():
                unquoted.append(line.strip())
        text = " ".join(" ".join(unquoted).split())
        if text:
            return text[:_MAX_SUGGESTION_TEXT_LENGTH]

    titles = [
        str(action.get("title") or action.get("command") or "").strip()
        for action in document.dispatched_actions
    ]
    fallback = " → ".join(title for title in titles if title)
    return (
        fallback[:_MAX_SUGGESTION_TEXT_LENGTH]
        or "Apply the selected stored recovery actions."
    )


def _suggestion_action(action: Mapping[str, Any]) -> Mapping[str, Any]:
    """Copy exact replay fields while dropping prior-run observations."""
    run_only_fields = {"status", "sent_at", "state_before"}
    return {
        key: copy.deepcopy(value)
        for key, value in action.items()
        if key not in run_only_fields
    }


def _memory_retrieval_text(document: MemoryDocument) -> str:
    """Return descriptive memory text without executable action payloads."""
    retrieval_fields = (
        "site",
        "room_number",
        "flow_id",
        "flow_name",
        "area_name",
        "item_name",
        "failure_status",
        "failed_command",
        "captured_at",
        "resolved_at",
    )
    metadata = {
        field_name: document.frontmatter.get(field_name)
        for field_name in retrieval_fields
        if document.frontmatter.get(field_name) is not None
    }
    descriptive_body = document.body.rsplit(
        "\n## Dispatched Actions\n",
        1,
    )[0].strip()
    return "\n".join(
        (
            "# Recovery memory",
            "",
            "## Retrieval metadata",
            "```json",
            json.dumps(
                metadata,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            "```",
            "",
            descriptive_body,
        )
    ).strip()


def _failure_retrieval_values(
    failure: Mapping[str, Any],
) -> dict[str, Any]:
    values = {
        "sysid": failure.get("sysid"),
        "flow_id": failure.get("flow_id"),
        "flow_name": failure.get("flow_name"),
        "failed_command": failure.get("action_command"),
        "site": failure.get("site"),
        "room_number": failure.get("room_number"),
        "area_name": failure.get("area_name"),
        "item_name": failure.get("item_name"),
    }
    context = failure.get("sanitized_context")
    if isinstance(context, Mapping):
        flow = context.get("flow")
        if isinstance(flow, Mapping):
            values["site"] = values["site"] or flow.get("site")
            values["room_number"] = values["room_number"] or flow.get("room")
        action = context.get("action")
        if isinstance(action, Mapping):
            values["area_name"] = values["area_name"] or action.get("area_name")
            values["item_name"] = values["item_name"] or action.get("item_name")
    return values


def _response_rows(response: Any) -> list[Mapping[str, Any]]:
    data = getattr(response, "data", None)
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, Mapping)]


def _safe_identifier(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return None
    text = str(value).strip()
    if not text:
        return None
    printable = "".join(
        character if character.isprintable() else "\ufffd"
        for character in text
    )
    return printable[:200]


def _normalized_text(value: Any) -> str:
    return str(value).strip().casefold() if value is not None else ""


def _bounded_matcher_message(value: str) -> str:
    flattened = " ".join(str(value).splitlines()).strip()
    printable = "".join(
        character if character.isprintable() else "\ufffd"
        for character in flattened
    )
    return printable[:_MAX_MATCHER_MESSAGE_LENGTH]


def _status_name(status: Any) -> str:
    value = getattr(status, "value", status)
    return str(value).rsplit(".", 1)[-1].upper()


def _positive_float(
    environment: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw_value = environment.get(name)
    if raw_value is None:
        return default
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a number") from error
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_int(
    environment: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    raw_value = environment.get(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value <= 0 or value > 10_000:
        raise ValueError(f"{name} must be between 1 and 10000")
    return value


AgentFactory = Callable[[ResolverSettings], FailureAgent]
MemoryStoreFactory = Callable[[ResolverSettings], MarkdownMemoryStore]


def _default_agent_factory(settings: ResolverSettings) -> FailureAgent:
    return OpenAIFailureAgent(
        model=settings.openai_model,
        api_key=settings.openai_api_key,
    )


def _default_memory_store_factory(
    settings: ResolverSettings,
) -> MarkdownMemoryStore:
    return GitMemoryStore(
        GitMemoryConfig(
            repo_url=settings.memory_repo_url,
            repo_root=settings.memory_repo_root,
            branch=settings.memory_repo_branch,
        )
    )


def create_app(
    *,
    settings: ResolverSettings | None = None,
    client_factory: ClientFactory = create_supabase_client,
    agent_factory: AgentFactory = _default_agent_factory,
    memory_store_factory: MemoryStoreFactory = _default_memory_store_factory,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        resolved_settings = settings or ResolverSettings.from_env()
        runtime = SupabaseAgentRuntime(
            resolved_settings,
            agent=agent_factory(resolved_settings),
            memory_store=memory_store_factory(resolved_settings),
            client_factory=client_factory,
        )
        application.state.resolver = runtime
        task = asyncio.create_task(
            runtime.run(),
            name="supabase-failure-resolver-agent",
        )
        application.state.resolver_task = task
        await asyncio.sleep(0)
        try:
            yield
        finally:
            await runtime.stop()
            try:
                await asyncio.wait_for(
                    task,
                    timeout=resolved_settings.shutdown_timeout_seconds,
                )
            except asyncio.TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    application = FastAPI(
        title="Billie Failure Resolver Agent",
        version="0.2.0",
        lifespan=lifespan,
    )

    @application.get("/health")
    async def health(request: Request) -> JSONResponse:
        runtime = getattr(request.app.state, "resolver", None)
        task = getattr(request.app.state, "resolver_task", None)
        if runtime is None:
            return JSONResponse(
                {"mode": "agent", "status": "starting", "connected": False},
                status_code=503,
            )
        snapshot = runtime.snapshot()
        live = runtime.state.running and not (task is not None and task.done())
        return JSONResponse(snapshot, status_code=200 if live else 503)

    @application.get("/readyz")
    async def ready(request: Request) -> JSONResponse:
        runtime = getattr(request.app.state, "resolver", None)
        snapshot = (
            runtime.snapshot()
            if runtime is not None
            else {"mode": "agent", "status": "starting", "connected": False}
        )
        return JSONResponse(
            snapshot,
            status_code=200 if snapshot["connected"] else 503,
        )

    return application


app = create_app()


if __name__ == "__main__":
    _configure_logging()
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "INFO").lower(),
    )
