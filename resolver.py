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
    MemoryChoice,
    MemoryChoiceResult,
    MemoryFinishAction,
    MemoryReadAction,
    MemoryRetrievalTurnResult,
    MemorySearchAction,
    OpenAIFailureAgent,
)
from episode_evidence import build_episode_evidence
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
from retrieval import (
    MAX_READ_DOCUMENTS,
    MAX_SEARCH_CALLS,
    MAX_SEARCH_RESULTS,
    MAX_TOTAL_READ_CHARS,
    MemoryRetrievalIndex,
)
from recovery_executor import (
    RecoveryCoordinator,
    RecoveryContractError,
    RecoveryExecutionSettings,
    SupabaseRecoveryDatabase,
    rebind_recovery_actions,
)


logger = logging.getLogger("failure_resolver.agent")

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESOLUTION_EVENTS = ("INSERT", "UPDATE")
_FAILURE_ID = "failure_id"
_RESOLUTION_ID = "resolution_id"
_COMPLETED_ANALYSIS_STATUS = "completed"
_MAX_MATCHER_MESSAGE_LENGTH = 800
_MAX_SUGGESTION_TEXT_LENGTH = 2_000
_MAX_RETRIEVAL_TURNS = 10
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

    async def next_memory_retrieval_turn(
        self,
        failure: Mapping[str, Any],
        observations: Sequence[Mapping[str, Any]],
    ) -> MemoryRetrievalTurnResult: ...

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

    async def alatest_memory_commit(
        self,
        resolution_id: str,
        *,
        refresh: bool = True,
    ) -> str | None: ...

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
    auto_execute: bool = False
    recovery_robot_allowlist: tuple[str, ...] = ()
    recovery_max_attempts: int = 3
    recovery_command_timeout_seconds: float = 15.0
    recovery_outcome_timeout_seconds: float = 60.0
    recovery_lease_seconds: int = 300
    recovery_reconcile_interval_seconds: float = 30.0
    recovery_start_grace_seconds: float = 5.0
    recovery_cf_access_client_id: str = field(default="", repr=False)
    recovery_cf_access_client_secret: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        # Validate the opt-in execution boundary even when settings are created
        # directly in tests or deployment code rather than through from_env.
        recovery = self.recovery_execution_settings()
        object.__setattr__(
            self,
            "recovery_robot_allowlist",
            recovery.robot_allowlist,
        )

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
        auto_execute = _boolean(
            environment,
            "RESOLVER_AUTO_EXECUTE",
            False,
        )
        recovery_allowlist = tuple(
            part.strip().upper()
            for part in environment.get(
                "RECOVERY_ROBOT_ALLOWLIST",
                "",
            ).split(",")
            if part.strip()
        )

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
            auto_execute=auto_execute,
            recovery_robot_allowlist=recovery_allowlist,
            recovery_max_attempts=_bounded_int(
                environment,
                "RECOVERY_MAX_ATTEMPTS",
                3,
                minimum=1,
                maximum=20,
            ),
            recovery_command_timeout_seconds=_positive_float(
                environment,
                "RECOVERY_COMMAND_TIMEOUT_SECONDS",
                15.0,
            ),
            recovery_outcome_timeout_seconds=_positive_float(
                environment,
                "RECOVERY_OUTCOME_TIMEOUT_SECONDS",
                60.0,
            ),
            recovery_lease_seconds=_bounded_int(
                environment,
                "RECOVERY_LEASE_SECONDS",
                300,
                minimum=5,
                maximum=900,
            ),
            recovery_reconcile_interval_seconds=_positive_float(
                environment,
                "RECOVERY_RECONCILE_INTERVAL_SECONDS",
                30.0,
            ),
            recovery_start_grace_seconds=_nonnegative_float(
                environment,
                "RECOVERY_START_GRACE_SECONDS",
                5.0,
            ),
            recovery_cf_access_client_id=environment.get(
                "RECOVERY_CF_ACCESS_CLIENT_ID",
                "",
            ).strip(),
            recovery_cf_access_client_secret=environment.get(
                "RECOVERY_CF_ACCESS_CLIENT_SECRET",
                "",
            ).strip(),
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

    def recovery_execution_settings(self) -> RecoveryExecutionSettings:
        return RecoveryExecutionSettings(
            enabled=self.auto_execute,
            robot_allowlist=self.recovery_robot_allowlist,
            max_attempts=self.recovery_max_attempts,
            command_timeout_seconds=self.recovery_command_timeout_seconds,
            outcome_timeout_seconds=self.recovery_outcome_timeout_seconds,
            lease_seconds=self.recovery_lease_seconds,
            cf_access_client_id=self.recovery_cf_access_client_id,
            cf_access_client_secret=self.recovery_cf_access_client_secret,
            reconcile_limit=self.reconcile_limit,
            reconcile_interval_seconds=(
                self.recovery_reconcile_interval_seconds
            ),
            start_grace_seconds=self.recovery_start_grace_seconds,
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
class RetrievalOutcome:
    choice: MemoryChoice
    documents: tuple[MemoryDocument, ...]


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
        if (
            _normalized_text(row.get("analysis_status"))
            != _COMPLETED_ANALYSIS_STATUS
        ):
            self.state.failures_skipped += 1
            return None
        if not await self._claim_failure(failure_id):
            self.state.failures_skipped += 1
            return None

        self.state.failures_claimed += 1
        self.state.last_failure_id = failure_id
        try:
            index = await self.memory_store.arebuild_index(refresh=True)
            safe_documents = tuple(
                document
                for document in index.values()
                if _is_safe_execution_candidate(document)
            )
            safe_documents = await self._without_revoked(safe_documents)
            if not safe_documents:
                await self._finish_failure(
                    failure_id,
                    "no_solution",
                    "No applicable recovery memory is available.",
                )
                self.state.no_solution += 1
                return None

            retrieval = await self._retrieve_memory_choice(
                _failure_with_location_context(row),
                safe_documents,
            )
            choice = retrieval.choice
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
                for document in retrieval.documents
            }.get(choice.memory_id or "")
            if selected is None or not _is_safe_execution_candidate(selected):
                raise AgentReasoningError(
                    "Selected memory is not an execution candidate"
                )

            # Deep-copy the immutable source document.  Nothing from the model
            # can enter this executable payload.
            remembered_actions = tuple(
                _suggestion_action(action)
                for action in selected.dispatched_actions
            )
            if not remembered_actions:
                raise AgentReasoningError(
                    "Selected memory contains no dispatched actions"
                )
            actions = remembered_actions
            if (
                remembered_actions[-1].get("command")
                in ("$rerun", "$resume_flow")
            ):
                flow_snapshot = row.get("flow_snapshot")
                try:
                    if not isinstance(flow_snapshot, Mapping):
                        raise RecoveryContractError(
                            "failure has no complete Flow snapshot"
                        )
                    actions = rebind_recovery_actions(
                        remembered_actions,
                        flow_snapshot=flow_snapshot,
                        flow_id=_required_recovery_text(row, "flow_id"),
                        action_index=_required_recovery_index(
                            row,
                            "action_index",
                        ),
                        action_command=_required_recovery_text(
                            row,
                            "action_command",
                        ),
                    )
                except RecoveryContractError as error:
                    await self._finish_failure(
                        failure_id,
                        "no_solution",
                        (
                            "No applicable solution: the remembered recovery "
                            f"cannot be safely bound to this Flow run "
                            f"({error})."
                        ),
                    )
                    self.state.no_solution += 1
                    return None
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

    async def _retrieve_memory_choice(
        self,
        failure: Mapping[str, Any],
        documents: Sequence[MemoryDocument],
    ) -> RetrievalOutcome:
        """Browse one immutable safe-memory snapshot and fail closed."""

        corpus = await asyncio.to_thread(
            MemoryRetrievalIndex,
            {
                document.resolution_id: document
                for document in documents
            },
        )
        auto_read = corpus.auto_read_eligibility(
            max_documents=MAX_READ_DOCUMENTS,
            max_total_chars=MAX_TOTAL_READ_CHARS,
        )
        if auto_read.eligible and auto_read.memory_ids:
            reads = corpus.read(
                auto_read.memory_ids,
                max_documents=MAX_READ_DOCUMENTS,
                max_total_chars=MAX_TOTAL_READ_CHARS,
            )
            choice_result = await self.agent.choose_memory(
                failure,
                [
                    MarkdownMemory(
                        memory_id=read.memory_id,
                        markdown=read.markdown,
                    )
                    for read in reads
                ],
            )
            by_id = {
                document.resolution_id: document
                for document in documents
            }
            return RetrievalOutcome(
                choice=choice_result.choice,
                documents=tuple(by_id[read.memory_id] for read in reads),
            )

        observations: list[Mapping[str, Any]] = [
            {
                "kind": "corpus",
                "eligible_memory_count": len(documents),
                "searches_remaining": MAX_SEARCH_CALLS,
                "reads_remaining": MAX_READ_DOCUMENTS,
            }
        ]
        discovered_ids: set[str] = set()
        read_documents: dict[str, MemoryDocument] = {}
        searches_used = 0
        read_chars = 0
        by_id = {
            document.resolution_id: document
            for document in documents
        }

        for _ in range(_MAX_RETRIEVAL_TURNS):
            result = await self.agent.next_memory_retrieval_turn(
                failure,
                observations,
            )
            step = result.turn.step
            if isinstance(step, MemorySearchAction):
                if searches_used >= MAX_SEARCH_CALLS:
                    return _retrieval_no_solution(
                        read_documents.values(),
                        "The memory search budget was exhausted.",
                    )
                searches_used += 1
                hits = await asyncio.to_thread(
                    corpus.search,
                    step.query,
                    hints=_failure_retrieval_values(failure),
                    limit=MAX_SEARCH_RESULTS,
                )
                discovered_ids.update(hit.memory_id for hit in hits)
                observations.append(
                    {
                        "kind": "search_results",
                        "query": step.query,
                        "searches_remaining": MAX_SEARCH_CALLS - searches_used,
                        "hits": [
                            {
                                "memory_id": hit.memory_id,
                                "score": hit.score,
                                "matched_fields": list(hit.matched_fields),
                                "metadata": dict(hit.metadata),
                                "snippet": hit.snippet,
                            }
                            for hit in hits
                        ],
                    }
                )
                continue

            if isinstance(step, MemoryReadAction):
                requested = [
                    memory_id
                    for memory_id in step.memory_ids
                    if memory_id not in read_documents
                ]
                if not requested:
                    observations.append(
                        {
                            "kind": "read_results",
                            "documents": [],
                            "note": "All requested memories were already read.",
                        }
                    )
                    continue
                if not set(requested).issubset(discovered_ids):
                    return _retrieval_no_solution(
                        read_documents.values(),
                        "The retrieval agent requested an undiscovered memory.",
                    )
                remaining_documents = MAX_READ_DOCUMENTS - len(read_documents)
                remaining_chars = MAX_TOTAL_READ_CHARS - read_chars
                if (
                    len(requested) > remaining_documents
                    or remaining_chars <= 0
                ):
                    return _retrieval_no_solution(
                        read_documents.values(),
                        "The memory read budget was exhausted.",
                    )
                try:
                    reads = corpus.read(
                        requested,
                        max_documents=remaining_documents,
                        max_total_chars=remaining_chars,
                    )
                except ValueError:
                    return _retrieval_no_solution(
                        read_documents.values(),
                        "The memory read budget was exhausted.",
                    )
                read_chars += sum(len(read.markdown) for read in reads)
                for read in reads:
                    read_documents[read.memory_id] = by_id[read.memory_id]
                observations.append(
                    {
                        "kind": "read_results",
                        "reads_remaining": (
                            MAX_READ_DOCUMENTS - len(read_documents)
                        ),
                        "documents": [
                            {
                                "memory_id": read.memory_id,
                                "markdown": read.markdown,
                            }
                            for read in reads
                        ],
                    }
                )
                continue

            if not isinstance(step, MemoryFinishAction):
                raise AgentReasoningError(
                    "Retrieval agent returned an unsupported operation"
                )
            choice = step.choice
            if (
                choice.decision == "no_solution"
                and (searches_used == 0 or not read_documents)
            ):
                observations.append(
                    {
                        "kind": "retrieval_correction",
                        "message": (
                            "Search and read at least one returned memory "
                            "before concluding that no solution exists."
                        ),
                    }
                )
                continue
            if (
                choice.decision == "apply_memory"
                and choice.memory_id not in read_documents
            ):
                return _retrieval_no_solution(
                    read_documents.values(),
                    "The retrieval agent selected an unread memory.",
                )
            return RetrievalOutcome(
                choice=choice,
                documents=tuple(read_documents.values()),
            )

        return _retrieval_no_solution(
            read_documents.values(),
            "The memory retrieval turn budget was exhausted.",
        )

    async def _without_revoked(
        self,
        documents: tuple[MemoryDocument, ...],
    ) -> tuple[MemoryDocument, ...]:
        """Drop memories whose source resolution an operator has revoked."""
        if not documents:
            return documents
        client = self._required_client()
        response = await (
            client.table(self.resolutions_table)
            .select("resolution_id")
            .in_(
                "resolution_id",
                [document.resolution_id for document in documents],
            )
            .not_.is_("revoked_at", "null")
            .execute()
        )
        revoked = {
            row.get("resolution_id")
            for row in (response.data or [])
            if row.get("resolution_id")
        }
        if not revoked:
            return documents
        logger.info(
            "Excluded %d revoked memories from matching",
            len(revoked),
        )
        return tuple(
            document
            for document in documents
            if document.resolution_id not in revoked
        )

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
        linked_failure = (
            await self._fetch_row(
                self.failure_events_table,
                _FAILURE_ID,
                failure_id,
            )
            if failure_id is not None
            else None
        )
        acknowledgement = await self._claim_memory_ingestion(
            failure_id,
            resolution_id,
            current_failure=linked_failure,
        )
        if acknowledgement == "owned_elsewhere":
            self.state.resolutions_skipped += 1
            return None

        try:
            episode_row = _resolution_with_failure_context(
                row,
                linked_failure,
            )
            memory_row = _row_with_successful_actions(episode_row)
            source = resolution_source_from_row(memory_row)
            if await self.memory_store.ahas_source_hash(
                source.resolution_id,
                source.source_hash,
                refresh=True,
            ):
                self.state.resolutions_skipped += 1
                commit_sha = self._known_commit_shas.get(
                    source.resolution_id
                ) or await self.memory_store.alatest_memory_commit(
                    source.resolution_id,
                    refresh=False,
                )
                if commit_sha is None:
                    raise AgentReasoningError(
                        "Existing memory has no Git commit provenance"
                    )
                if acknowledgement == "claimed":
                    await self._mark_memory_ingested(
                        failure_id,
                        source.resolution_id,
                        commit_sha=commit_sha,
                    )
                elif _memory_ack_needs_commit_refresh(
                    linked_failure,
                    source.resolution_id,
                    commit_sha,
                ):
                    await self._refresh_memory_ingested(
                        failure_id,
                        source.resolution_id,
                        commit_sha=commit_sha,
                        expected_commit_sha=_memory_ack_commit(linked_failure),
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
            elif (
                result.changed
                and result.commit_sha is not None
                and _is_ingested_memory_ack(
                    linked_failure,
                    source.resolution_id,
                )
            ):
                await self._refresh_memory_ingested(
                    failure_id,
                    source.resolution_id,
                    commit_sha=result.commit_sha,
                    expected_commit_sha=_memory_ack_commit(linked_failure),
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
            .eq("analysis_status", _COMPLETED_ANALYSIS_STATUS)
            .execute()
        )
        data = getattr(response, "data", None)
        return isinstance(data, list) and bool(data)

    async def _claim_memory_ingestion(
        self,
        failure_id: str | None,
        resolution_id: str,
        *,
        current_failure: Mapping[str, Any] | None = None,
    ) -> Literal["claimed", "owned_elsewhere", "untracked"]:
        if failure_id is None:
            return "untracked"
        current = (
            dict(current_failure)
            if current_failure is not None
            else await self._fetch_row(
                self.failure_events_table,
                _FAILURE_ID,
                failure_id,
            )
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
                and current_status == "ingesting"
            ):
                return "owned_elsewhere"
            # An already-ingested resolution must still pass through source
            # hashing. Linked episode evidence can arrive or improve after the
            # first memory write; Git will make an unchanged source a no-op and
            # rewrite the same resolution document when its evidence changed.
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

    async def _refresh_memory_ingested(
        self,
        failure_id: str | None,
        resolution_id: str,
        *,
        commit_sha: str,
        expected_commit_sha: str | None,
    ) -> None:
        """CAS-refresh provenance for an already-ingested changed memory."""

        if failure_id is None:
            return
        client = self._required_client()
        query = (
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
            .eq("memory_status", "ingested")
            .eq("memory_resolution_id", resolution_id)
        )
        query = (
            query.eq("memory_commit_sha", expected_commit_sha)
            if expected_commit_sha is not None
            else query.is_("memory_commit_sha", "null")
        )
        response = await query.execute()
        data = getattr(response, "data", None)
        if isinstance(data, list) and data:
            self.state.memory_acks_ingested += 1

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
        recovery_coordinator: RecoveryCoordinator | None = None,
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
        self.recovery_coordinator = recovery_coordinator or RecoveryCoordinator(
            settings.recovery_execution_settings()
        )
        # The base observer serializes this queue through one worker, which
        # keeps Git writes and status transitions ordered.
        self._fetch_queue = asyncio.Queue(
            maxsize=max(256, settings.reconcile_limit * 2 + 32)
        )

    async def run(self) -> None:
        await self.recovery_coordinator.start()
        try:
            await super().run()
        finally:
            await self.recovery_coordinator.stop()
            await self.processor.close()

    async def stop(self) -> None:
        # Stop command work while the current Supabase client can still record
        # an interrupted in-flight attempt as unknown.
        await self.recovery_coordinator.stop()
        await super().stop()

    async def _connect(self) -> None:
        await super()._connect()
        self.processor.client = self._client
        if self._client is None:
            raise ConnectionError("Supabase client is unavailable")
        self.recovery_coordinator.set_database(
            SupabaseRecoveryDatabase(
                self._client,
                failure_events_table=(
                    self.resolver_settings.failure_events_table
                ),
            )
        )
        await self._connect_resolution_channel()
        await self._enqueue_reconciliation()
        await self.recovery_coordinator.reconcile()

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
        # Let an in-flight automatic recovery attach a newly detected
        # recurrence immediately, before another matcher decision can replace
        # the session-pinned plan or reset its retry budget.
        self.recovery_coordinator.notify_failure(signal.failure_id)
        pending_resolution = await self.processor.pending_memory_resolution(
            signal.failure_id
        )
        if pending_resolution is not None:
            await self.processor.learn_resolution(pending_resolution)
        await self.processor.process_failure(signal.failure_id)
        # A previously unprepared event becomes executable only after the
        # matcher transaction pins a solution candidate.
        self.recovery_coordinator.notify_failure(signal.failure_id)
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
            .eq("analysis_status", _COMPLETED_ANALYSIS_STATUS)
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
        self.recovery_coordinator.set_database(None)
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
        snapshot.update(self.recovery_coordinator.snapshot())
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
        signature=generalization.signature.model_dump(mode="json"),
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


def _resolution_with_failure_context(
    resolution: Mapping[str, Any],
    failure: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Join one resolution to its immutable failure episode evidence.

    Resolution values and action records remain authoritative.  Failure values
    only fill absent retrieval/generalization fields; both original rows are
    retained separately as bounded, credential-free episode evidence.
    """

    prepared = copy.deepcopy(dict(resolution))
    if failure is not None:
        failure_context = _project_failure_context(failure)
        for field_name, value in failure_context.items():
            if _is_missing_context_value(prepared.get(field_name)):
                prepared[field_name] = copy.deepcopy(value)
    prepared["episode_evidence"] = build_episode_evidence(
        failure_event=failure,
        resolution_event=resolution,
    )
    return prepared


def _project_failure_context(failure: Mapping[str, Any]) -> dict[str, Any]:
    projected: dict[str, Any] = {}
    direct_fields = (
        "sysid",
        "site_id",
        "site",
        "floor",
        "room_number",
        "map_id",
        "map_name",
        "map_observed_at",
        "flow_id",
        "flow_name",
        "activity_id",
        "area_name",
        "item_name",
        "robot_version",
        "robot_status",
        "navigation",
        "arm_state",
        "mapping_pose",
        "status_reported_at",
        "flow_snapshot",
        "run_code",
    )
    for field_name in direct_fields:
        if not _is_missing_context_value(failure.get(field_name)):
            projected[field_name] = failure.get(field_name)

    aliases = {
        "failure_status": ("failure_status", "flow_status", "action_status"),
        "failure_reason": (
            "failure_reason",
            "description",
            "reported_cause",
            "reported_error",
        ),
        "failed_command": ("failed_command", "action_command"),
        "failed_action": ("failed_action", "failed_step"),
        "description": ("description", "failure_reason"),
        "failed_step": ("failed_step",),
        "robot_message": ("robot_message", "reported_error"),
        "auto_failure_reason": ("auto_failure_reason", "reported_cause"),
    }
    for target, candidates in aliases.items():
        for candidate in candidates:
            value = failure.get(candidate)
            if not _is_missing_context_value(value):
                projected[target] = value
                break

    context = failure.get("sanitized_context")
    if isinstance(context, Mapping):
        location = context.get("location")
        if isinstance(location, Mapping):
            for field_name in (
                "site_id",
                "site",
                "floor",
                "room_number",
                "map_id",
                "map_name",
                "map_observed_at",
            ):
                value = location.get(field_name)
                if (
                    field_name not in projected
                    and not _is_missing_context_value(value)
                ):
                    projected[field_name] = value
        flow = context.get("flow")
        if isinstance(flow, Mapping):
            for target, source in (
                ("site", "site"),
                ("room_number", "room"),
            ):
                value = flow.get(source)
                if (
                    target not in projected
                    and not _is_missing_context_value(value)
                ):
                    projected[target] = value
        action = context.get("action")
        if isinstance(action, Mapping):
            for field_name in ("area_name", "item_name"):
                value = action.get(field_name)
                if (
                    field_name not in projected
                    and not _is_missing_context_value(value)
                ):
                    projected[field_name] = value
        if "navigation" not in projected and isinstance(
            context.get("navigation"),
            Mapping,
        ):
            projected["navigation"] = context["navigation"]
    return projected


def _is_missing_context_value(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _is_ingested_memory_ack(
    failure: Mapping[str, Any] | None,
    resolution_id: str,
) -> bool:
    return bool(
        failure is not None
        and _normalized_text(failure.get("memory_status")) == "ingested"
        and _safe_identifier(failure.get("memory_resolution_id"))
        == resolution_id
    )


def _memory_ack_needs_commit_refresh(
    failure: Mapping[str, Any] | None,
    resolution_id: str,
    commit_sha: str,
) -> bool:
    return bool(
        _is_ingested_memory_ack(failure, resolution_id)
        and _safe_identifier(failure.get("memory_commit_sha"))
        != commit_sha
    )


def _memory_ack_commit(
    failure: Mapping[str, Any] | None,
) -> str | None:
    return (
        _safe_identifier(failure.get("memory_commit_sha"))
        if failure is not None
        else None
    )


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


def _required_recovery_text(
    row: Mapping[str, Any],
    field_name: str,
) -> str:
    value = row.get(field_name)
    if not isinstance(value, str) or not value or value.strip() != value:
        raise RecoveryContractError(
            f"failure {field_name} is missing or invalid"
        )
    return value


def _required_recovery_index(
    row: Mapping[str, Any],
    field_name: str,
) -> int:
    value = row.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RecoveryContractError(
            f"failure {field_name} is missing or invalid"
        )
    return value


def _retrieval_no_solution(
    documents: Iterable[MemoryDocument],
    reason: str,
) -> RetrievalOutcome:
    return RetrievalOutcome(
        choice=MemoryChoice(
            decision="no_solution",
            memory_id=None,
            confidence=0.0,
            reason=reason,
        ),
        documents=tuple(documents),
    )


def _failure_retrieval_values(
    failure: Mapping[str, Any],
) -> dict[str, Any]:
    values = {
        "sysid": failure.get("sysid"),
        "site_id": failure.get("site_id"),
        "site": failure.get("site"),
        "floor": failure.get("floor"),
        "room_number": failure.get("room_number"),
        "map_id": failure.get("map_id"),
        "map_name": failure.get("map_name"),
        "flow_id": failure.get("flow_id"),
        "flow_name": failure.get("flow_name"),
        "activity_id": failure.get("activity_id"),
        "area_name": failure.get("area_name"),
        "item_name": failure.get("item_name"),
        "failed_command": (
            failure.get("failed_command")
            or failure.get("action_command")
        ),
    }
    navigation = failure.get("navigation")
    if isinstance(navigation, Mapping):
        current_map = navigation.get("current_map")
        if isinstance(current_map, Mapping):
            values["map_id"] = values["map_id"] or current_map.get("id")
            values["map_name"] = (
                values["map_name"] or current_map.get("map_name")
            )

    context = failure.get("sanitized_context")
    if isinstance(context, Mapping):
        location = context.get("location")
        if isinstance(location, Mapping):
            for field_name in (
                "site_id",
                "site",
                "floor",
                "room_number",
                "map_id",
                "map_name",
            ):
                values[field_name] = (
                    values[field_name] or location.get(field_name)
                )
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


def _nonnegative_float(
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
    if value < 0:
        raise ValueError(f"{name} must be zero or greater")
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


def _bounded_int(
    environment: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw_value = environment.get(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value < minimum or value > maximum:
        raise ValueError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


def _boolean(
    environment: Mapping[str, str],
    name: str,
    default: bool,
) -> bool:
    raw_value = environment.get(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().casefold()
    if normalized in ("true", "1", "yes", "on"):
        return True
    if normalized in ("false", "0", "no", "off"):
        return False
    raise ValueError(f"{name} must be a boolean")


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
