from __future__ import annotations

import asyncio
import copy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from agent import (
    AgentReasoningError,
    FailureSignature,
    GeneralizationResult,
    MemoryChoice,
    MemoryChoiceResult,
    MemoryFinishAction,
    MemoryReadAction,
    MemoryRetrievalTurn,
    MemoryRetrievalTurnResult,
    MemorySearchAction,
    ResolutionGeneralization,
)
from memory_store import MemoryDocument, MemoryWriteResult
from recovery_executor import RecoveryExecutionState
from resolver import (
    FailureResolverProcessor,
    ResolverSettings,
    SupabaseAgentRuntime,
    _failure_retrieval_values,
)


FAILURE_ID = "99e7f23d-64a7-4cd8-a0d8-e36154122f78"
RESOLUTION_ID = "5b5bbdc0-7c96-4bd8-9c6b-6ec989f3275e"


class FakeResponse:
    def __init__(self, data: Any) -> None:
        self.data = data


class FakeQuery:
    def __init__(self, client: "FakeClient", table: str) -> None:
        self.client = client
        self.table = table
        self.columns: str | None = None
        self.filters: list[tuple[str, Any]] = []
        self.predicates: list[Any] = []
        self._negate_next = False
        self.limit_value: int | None = None
        self.order_column: str | None = None
        self.update_values: dict[str, Any] | None = None

    def select(self, columns: str) -> "FakeQuery":
        self.columns = columns
        return self

    def eq(self, column: str, value: Any) -> "FakeQuery":
        self.filters.append((column, value))
        return self

    def is_(self, column: str, value: Any) -> "FakeQuery":
        target = None if value == "null" else value
        if self._negate_next:
            self._negate_next = False
            self.predicates.append(
                lambda row, c=column, t=target: row.get(c) != t
            )
        else:
            self.filters.append((column, target))
        return self

    def in_(self, column: str, values: Any) -> "FakeQuery":
        allowed = list(values)
        self.predicates.append(
            lambda row, c=column, a=allowed: row.get(c) in a
        )
        return self

    @property
    def not_(self) -> "FakeQuery":
        self._negate_next = True
        return self

    def limit(self, value: int) -> "FakeQuery":
        self.limit_value = value
        return self

    def order(self, column: str) -> "FakeQuery":
        self.order_column = column
        return self

    def update(self, values: Mapping[str, Any]) -> "FakeQuery":
        self.update_values = dict(values)
        return self

    async def execute(self) -> FakeResponse:
        self.client.queries.append(self)
        rows = [
            row
            for row in self.client.tables.get(self.table, [])
            if all(row.get(column) == value for column, value in self.filters)
            and all(predicate(row) for predicate in self.predicates)
        ]
        if self.order_column is not None:
            rows = sorted(
                rows,
                key=lambda row: str(row.get(self.order_column) or ""),
            )
        if self.limit_value is not None:
            rows = rows[: self.limit_value]
        if self.update_values is not None:
            for row in rows:
                row.update(copy.deepcopy(self.update_values))
            self.client.updates.append(
                (self.table, copy.deepcopy(self.update_values), list(self.filters))
            )
        if self.columns and self.columns != "*":
            selected_columns = self.columns.split(",")
            return FakeResponse(
                [
                    {column: row.get(column) for column in selected_columns}
                    for row in rows
                ]
            )
        return FakeResponse([copy.deepcopy(row) for row in rows])


class FakeRealtime:
    def __init__(self) -> None:
        self.is_connected = True

    async def close(self) -> None:
        self.is_connected = False


class FakeChannel:
    def __init__(self) -> None:
        self.is_joined = False
        self.bindings: dict[str, Any] = {}

    def on_postgres_changes(
        self,
        event: str,
        *,
        schema: str,
        table: str,
        callback: Any,
    ) -> "FakeChannel":
        self.bindings[event] = {
            "schema": schema,
            "table": table,
            "callback": callback,
        }
        return self

    async def subscribe(self, callback: Any) -> None:
        self.is_joined = True
        callback("SUBSCRIBED", None)

    def emit(self, event: str, record: Mapping[str, Any]) -> None:
        self.bindings[event]["callback"](
            {
                "data": {
                    "type": event,
                    "commit_timestamp": "2026-07-28T18:00:00Z",
                    "record": dict(record),
                }
            }
        )


class FakeClient:
    def __init__(self, tables: Mapping[str, list[dict[str, Any]]]) -> None:
        self.tables = {
            table: [copy.deepcopy(row) for row in rows]
            for table, rows in tables.items()
        }
        self.queries: list[FakeQuery] = []
        self.updates: list[
            tuple[str, dict[str, Any], list[tuple[str, Any]]]
        ] = []
        self.channels: dict[str, FakeChannel] = {}
        self.realtime = FakeRealtime()

    def table(self, table: str) -> FakeQuery:
        return FakeQuery(self, table)

    def channel(self, name: str) -> FakeChannel:
        channel = FakeChannel()
        self.channels[name] = channel
        return channel

    async def remove_channel(self, channel: FakeChannel) -> None:
        channel.is_joined = False


class FakeAgent:
    def __init__(
        self,
        *,
        choice: MemoryChoice | None = None,
        retrieval_turns: list[MemoryRetrievalTurn] | None = None,
        generalization: ResolutionGeneralization | None = None,
        generalization_error: Exception | None = None,
    ) -> None:
        self.choice = choice
        self.retrieval_turns = list(retrieval_turns or [])
        self.generalization = generalization
        self.generalization_error = generalization_error
        self.choice_calls: list[tuple[Mapping[str, Any], Any]] = []
        self.retrieval_calls: list[
            tuple[Mapping[str, Any], list[Mapping[str, Any]]]
        ] = []
        self.generalization_calls: list[Mapping[str, Any]] = []
        self.close_calls = 0

    async def choose_memory(self, failure, memories) -> MemoryChoiceResult:
        self.choice_calls.append((copy.deepcopy(failure), copy.deepcopy(memories)))
        if self.choice is None:
            raise AssertionError("choose_memory was not expected")
        return MemoryChoiceResult(
            choice=self.choice,
            model="test-model",
            response_id="resp-choice",
            input_tokens=10,
            output_tokens=5,
        )

    async def next_memory_retrieval_turn(
        self,
        failure,
        observations,
    ) -> MemoryRetrievalTurnResult:
        self.retrieval_calls.append(
            (copy.deepcopy(failure), copy.deepcopy(list(observations)))
        )
        if not self.retrieval_turns:
            raise AssertionError("retrieval turn was not expected")
        return MemoryRetrievalTurnResult(
            turn=self.retrieval_turns.pop(0),
            model="test-model",
            response_id="resp-retrieval",
            input_tokens=10,
            output_tokens=5,
        )

    async def generalize_resolution(self, resolution) -> GeneralizationResult:
        self.generalization_calls.append(copy.deepcopy(resolution))
        if self.generalization_error is not None:
            raise self.generalization_error
        if self.generalization is None:
            raise AssertionError("generalize_resolution was not expected")
        demonstrated = tuple(
            copy.deepcopy(action)
            for action in resolution["action_runs"]
            if action.get("status") == "sent"
        )
        return GeneralizationResult(
            generalization=self.generalization,
            demonstrated_actions=demonstrated,
            model="test-model",
            response_id="resp-generalize",
            input_tokens=20,
            output_tokens=8,
        )

    async def close(self) -> None:
        self.close_calls += 1


class FakeMemoryStore:
    def __init__(
        self,
        *,
        index: Mapping[str, MemoryDocument] | None = None,
        has_source_hash: bool = False,
        latest_commit: str | None = "f" * 40,
    ) -> None:
        self.index = dict(index or {})
        self.has_source_hash_result = has_source_hash
        self.latest_commit_result = latest_commit
        self.index_calls = 0
        self.hash_calls: list[tuple[str, str, bool]] = []
        self.latest_commit_calls: list[tuple[str, bool]] = []
        self.writes = []

    async def arebuild_index(self, *, refresh: bool = True):
        self.index_calls += 1
        return self.index

    async def ahas_source_hash(
        self,
        resolution_id: str,
        source_hash: str,
        *,
        refresh: bool = True,
    ) -> bool:
        self.hash_calls.append((resolution_id, source_hash, refresh))
        return self.has_source_hash_result

    async def alatest_memory_commit(
        self,
        resolution_id: str,
        *,
        refresh: bool = True,
    ) -> str | None:
        self.latest_commit_calls.append((resolution_id, refresh))
        return self.latest_commit_result

    async def awrite_memory(self, draft):
        self.writes.append(draft)
        return MemoryWriteResult(
            resolution_id=draft.source.resolution_id,
            relative_path=f"memories/{draft.source.resolution_id}.md",
            changed=True,
            commit_sha="abc123",
        )


class FakeRecoveryCoordinator:
    def __init__(self) -> None:
        self.state = RecoveryExecutionState()
        self.started = False
        self.start_calls = 0
        self.stop_calls = 0
        self.reconcile_calls = 0
        self.databases: list[Any] = []
        self.failure_ids: list[str] = []

    async def start(self) -> None:
        if self.started:
            return
        self.started = True
        self.start_calls += 1

    async def stop(self) -> None:
        if not self.started:
            return
        self.started = False
        self.stop_calls += 1

    def set_database(self, database: Any) -> None:
        self.databases.append(database)

    async def reconcile(self) -> None:
        self.reconcile_calls += 1

    def notify_failure(self, failure_id: str) -> None:
        self.failure_ids.append(failure_id)

    def snapshot(self) -> dict[str, Any]:
        return self.state.snapshot(enabled=True)


def failure_row() -> dict[str, Any]:
    return {
        "failure_id": FAILURE_ID,
        "sysid": "BILLIE-16",
        "incident_key": "incident-1",
        "flow_id": "flow-room-101",
        "flow_name": "Open room drawer",
        "flow_status": "error",
        "failure_kind": "action_failed",
        "action_index": 4,
        "action_command": "open_drawer",
        "description": "The drawer was already open.",
        "failed_step": "Open drawer",
        "reported_cause": "drawer already open",
        "analysis_status": "completed",
        "matcher_status": "pending",
        "matcher_message": None,
        "resolver_suggestion": None,
        "memory_status": None,
        "memory_resolution_id": None,
        "memory_commit_sha": None,
        "memory_message": None,
        "memory_ingested_at": None,
        "site_id": 0,
        "site": "Office",
        "room_number": "101",
        "map_id": 39,
        "map_name": "BILLIE-17-OFFICE",
        "map_observed_at": "2026-07-28T17:59:58Z",
        "flow_snapshot": {
            "id": "flow-room-101",
            "status": "paused",
            "current_action_index": 4,
            "areas": [
                {
                    "name": "Closet",
                    "items": [
                        {
                            "name": "Drawer",
                            "actions": [
                                {
                                    "command": "approach_drawer",
                                    "action_index": 3,
                                    "status": "done",
                                },
                                {
                                    "command": "open_drawer",
                                    "action_index": 4,
                                    "status": "aborted",
                                },
                                {
                                    "command": "close_drawer",
                                    "action_index": 5,
                                    "status": "not_started",
                                },
                            ],
                        }
                    ],
                }
            ],
        },
        "sanitized_context": {
            "flow": {"site": "Office", "room": "101"},
            "action": {"area_name": "Closet", "item_name": "Drawer"},
        },
        "created_at": "2026-07-28T18:00:00Z",
    }


def test_retrieval_hints_include_flow_site_room_and_map_context() -> None:
    failure = failure_row()
    failure.update(
        {
            "site_id": None,
            "site": None,
            "floor": None,
            "room_number": None,
            "map_id": None,
            "map_name": None,
            "navigation": {
                "current_map": {
                    "id": 92,
                    "map_name": "hotel-floor-nine",
                }
            },
            "sanitized_context": {
                "location": {
                    "site_id": 12,
                    "site": "Test Hotel",
                    "floor": "9",
                    "room_number": "914",
                },
                "action": {
                    "area_name": "Entry",
                    "item_name": "Door",
                },
            },
        }
    )

    assert _failure_retrieval_values(failure) == {
        "sysid": "BILLIE-16",
        "site_id": 12,
        "site": "Test Hotel",
        "floor": "9",
        "room_number": "914",
        "map_id": 92,
        "map_name": "hotel-floor-nine",
        "flow_id": "flow-room-101",
        "flow_name": "Open room drawer",
        "activity_id": None,
        "area_name": "Entry",
        "item_name": "Door",
        "failed_command": "open_drawer",
    }


def resolution_row(
    *,
    outcome: str = "resolved",
    applied: bool = True,
) -> dict[str, Any]:
    return {
        "resolution_id": RESOLUTION_ID,
        "failure_id": FAILURE_ID,
        "sysid": "BILLIE-16",
        "site_id": 0,
        "site": "Office",
        "room_number": "101",
        "flow_id": "flow-room-101",
        "flow_name": "Open room drawer",
        "area_name": "Closet",
        "item_name": "Drawer",
        "failure_status": "error",
        "failure_reason": "The drawer was already open.",
        "failed_command": "open_drawer",
        "resolution": "Retry the interrupted drawer step.",
        "navigation": {
            "current_map": {"id": 39, "map_name": "BILLIE-17-OFFICE"}
        },
        "action_runs": [
            {
                "command": "$rerun",
                "title": "Retry",
                "arguments": {},
                "status": "sent",
                "retry_context": {
                    "retried_action": {
                        "command": "open_drawer",
                        "actionIndex": 4,
                        "areaName": "Closet",
                        "itemName": "Drawer",
                    },
                    "expected_next_action": {
                        "command": "close_drawer",
                        "actionIndex": 5,
                        "areaName": "Closet",
                        "itemName": "Drawer",
                    },
                },
            },
            {
                "command": "bump",
                "title": "Bump",
                "arguments": {},
                "status": "failed",
            },
        ],
        "outcome": outcome,
        "applied": applied,
        "created_at": "2026-07-28T18:01:00Z",
    }


def generalization() -> ResolutionGeneralization:
    return ResolutionGeneralization(
        failure_pattern=(
            "The drawer-opening step failed while the drawer was already open."
        ),
        resolution_summary="Retry the interrupted drawer step.",
        tags=["drawer", "already-open"],
        signature=FailureSignature(
            task_family="open drawer",
            failed_step="open drawer",
            failure_mode="already open",
            object_state="open",
            context=["closet"],
        ),
    )


def memory_document(
    tmp_path: Path,
    *,
    actions: tuple[Mapping[str, Any], ...],
    resolution_id: str = RESOLUTION_ID,
    failure_text: str = "The drawer was already open.",
) -> MemoryDocument:
    path = tmp_path / f"{resolution_id}.md"
    body = (
        "# Drawer memory\n\n"
        "## Failure Pattern\n"
        f"> {failure_text}\n\n"
        "## Recovery Knowledge\n"
        "> Confirm the drawer state, then retry the interrupted step.\n\n"
        "## Dispatched Actions\n"
        "```json\n"
        '[{"command":"must-not-enter-model-prompt","secret":"hidden"}]\n'
        "```\n"
    )
    path.write_text(body, encoding="utf-8")
    return MemoryDocument(
        path=path,
        frontmatter={
            "resolution_id": resolution_id,
            "source_hash": "a" * 64,
            "memory_kind": "positive",
            "actionable": True,
            "outcome": "resolved",
            "applied": True,
            "sysid": "BILLIE-16",
            "site": "Office",
            "room_number": "101",
            "flow_id": "flow-room-101",
            "flow_name": "Open room drawer",
            "area_name": "Closet",
            "item_name": "Drawer",
            "failed_command": "open_drawer",
        },
        dispatched_actions=actions,
        body=body,
    )


def processor(
    client: FakeClient,
    agent: FakeAgent,
    store: FakeMemoryStore,
    dev_store: FakeMemoryStore | None = None,
) -> FailureResolverProcessor:
    return FailureResolverProcessor(
        failure_events_table="failure_events",
        resolutions_table="flow_failure_resolutions",
        agent=agent,
        memory_store=store,
        dev_memory_store=dev_store,
        client=client,
    )


def settings(**overrides: Any) -> ResolverSettings:
    values = {
        "supabase_url": "https://project.supabase.co",
        "supabase_service_role_key": "service-secret",
        "openai_api_key": "openai-secret",
        "openai_model": "test-model",
        "memory_repo_url": "https://github.com/example/memory.git",
        "memory_repo_root": Path("/private/tmp/test-memory-checkout"),
        "failure_events_table": "failure_events",
        "resolutions_table": "flow_failure_resolutions",
        "reconcile_limit": 10,
        "reconnect_initial_seconds": 0.001,
        "reconnect_max_seconds": 0.002,
        "subscription_timeout_seconds": 0.1,
        "connection_check_seconds": 0.001,
        "disconnect_grace_seconds": 0.01,
        "shutdown_timeout_seconds": 0.1,
    }
    values.update(overrides)
    return ResolverSettings(**values)


async def wait_until(predicate, *, timeout: float = 0.5) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition did not become true")
        await asyncio.sleep(0.001)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_settings_require_agent_mode_and_hide_all_secrets() -> None:
    configured = ResolverSettings.from_env(
        {
            "RESOLVER_MODE": "agent",
            "SUPABASE_URL": "https://project.supabase.co/",
            "SUPABASE_SERVICE_ROLE_KEY": "supabase-secret",
            "OPENAI_API_KEY": "openai-secret",
            "OPENAI_MODEL": "test-model",
            "MEMORY_REPO_URL": "https://token@github.com/example/memory.git",
            "MEMORY_REPO_ROOT": "/private/tmp/memory",
        }
    )

    assert configured.supabase_url == "https://project.supabase.co"
    assert configured.openai_model == "test-model"
    rendered = repr(configured)
    assert "supabase-secret" not in rendered
    assert "openai-secret" not in rendered
    assert "token@" not in rendered

    with pytest.raises(ValueError, match="RESOLVER_MODE=agent"):
        ResolverSettings.from_env(
            {
                "RESOLVER_MODE": "observe",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "secret",
                "OPENAI_API_KEY": "secret",
                "MEMORY_REPO_URL": "https://github.com/example/memory.git",
            }
        )


@pytest.mark.anyio
async def test_match_skips_failure_until_analysis_is_completed() -> None:
    failure = failure_row()
    failure["analysis_status"] = "analyzing"
    client = FakeClient({"failure_events": [failure]})
    agent = FakeAgent()
    store = FakeMemoryStore()
    service = processor(client, agent, store)

    match = await service.process_failure(FAILURE_ID)

    assert match is None
    assert client.tables["failure_events"][0]["matcher_status"] == "pending"
    assert client.updates == []
    assert store.index_calls == 0
    assert agent.choice_calls == []
    assert agent.retrieval_calls == []
    assert service.state.failures_skipped == 1


@pytest.mark.anyio
async def test_match_selects_one_existing_id_and_returns_exact_stored_actions(
    tmp_path: Path,
) -> None:
    source_actions = (
        {
            "command": "$rerun",
            "title": "Retry",
            "arguments": {},
            "status": "sent",
            "sent_at": "2026-07-28T18:00:15Z",
            "state_before": {"status": "error"},
            "retry_context": {
                    "retried_action": {
                        "command": "open_drawer",
                        "actionIndex": 4,
                        "areaName": "Closet",
                        "itemName": "Drawer",
                    },
                    "expected_next_action": {
                        "command": "close_drawer",
                        "actionIndex": 5,
                        "areaName": "Closet",
                        "itemName": "Drawer",
                    },
                },
        },
    )
    document = memory_document(tmp_path, actions=source_actions)
    store = FakeMemoryStore(index={RESOLUTION_ID: document})
    agent = FakeAgent(
        choice=MemoryChoice(
            decision="apply_memory",
            memory_id=RESOLUTION_ID,
            confidence=0.91,
            reason="The failed step and drawer state match.",
        )
    )
    client = FakeClient({"failure_events": [failure_row()]})
    service = processor(client, agent, store)

    match = await service.process_failure(FAILURE_ID)

    assert match is not None
    assert match.memory_id == RESOLUTION_ID
    assert match.suggested_fix == (
        "Confirm the drawer state, then retry the interrupted step."
    )
    assert match.actions == (
        {
            "command": "$rerun",
            "title": "Retry",
            "arguments": {},
            "retry_context": {
                "retried_action": {
                    "command": "open_drawer",
                    "actionIndex": 4,
                    "areaName": "Closet",
                    "itemName": "Drawer",
                },
                "expected_next_action": {
                    "command": "close_drawer",
                    "actionIndex": 5,
                    "areaName": "Closet",
                    "itemName": "Drawer",
                },
            },
        },
    )
    assert match.actions[0] is not source_actions[0]
    persisted = client.tables["failure_events"][0]
    assert persisted["matcher_status"] == "solution_found"
    assert persisted["matcher_message"] == (
        "Suggested fix: Confirm the drawer state, then retry the interrupted "
        "step."
    )
    assert persisted["resolver_suggestion"] == {
        "memory_id": RESOLUTION_ID,
        "summary": (
            "Confirm the drawer state, then retry the interrupted step."
        ),
        "reason": "The failed step and drawer state match.",
        "confidence": 0.91,
        "actions": list(match.actions),
    }
    assert [update[1]["matcher_status"] for update in client.updates] == [
        "matching",
        "solution_found",
    ]
    assert ("analysis_status", "completed") in client.updates[0][2]
    assert client.updates[-1][1]["resolver_suggestion"] == (
        persisted["resolver_suggestion"]
    )

    failure_input, supplied_memories = agent.choice_calls[0]
    assert supplied_memories[0].memory_id == RESOLUTION_ID
    assert "must-not-enter-model-prompt" not in supplied_memories[0].markdown
    assert "Dispatched Actions" not in supplied_memories[0].markdown
    assert "The drawer was already open." in supplied_memories[0].markdown
    assert failure_input["sanitized_context"]["location"] == {
        "site_id": 0,
        "site": "Office",
        "room_number": "101",
        "map_id": 39,
        "map_name": "BILLIE-17-OFFICE",
        "map_observed_at": "2026-07-28T17:59:58Z",
    }

    match.actions[0]["retry_context"]["retried_action"]["command"] = "tampered"
    assert (
        source_actions[0]["retry_context"]["retried_action"]["command"]
        == "open_drawer"
    )


@pytest.mark.anyio
async def test_match_rebinds_modern_continuation_to_new_flow_run(
    tmp_path: Path,
) -> None:
    old_arguments = {
        "mode": "retry_current",
        "rewind_steps": 0,
        "current_action_index": 4,
        "current_command": "open_drawer",
        "flow_id": "old-flow-run",
    }
    source_actions = (
        {
            "command": "fold",
            "title": "Fold",
            "arguments": {"wait": True},
            "status": "sent",
        },
        {
            "command": "$resume_flow",
            "title": "Continue",
            "arguments": old_arguments,
            "arguments_effective": old_arguments,
            "explicit_arguments": list(old_arguments),
            "status": "sent",
            "continuation_context": {
                "expected_arguments": old_arguments,
                "current_action": {
                    "command": "open_drawer",
                    "actionIndex": 4,
                    "areaName": "Closet",
                    "itemName": "Drawer",
                },
                "target_action": {
                    "command": "open_drawer",
                    "actionIndex": 4,
                    "areaName": "Closet",
                    "itemName": "Drawer",
                },
            },
        },
    )
    document = memory_document(tmp_path, actions=source_actions)
    service = processor(
        FakeClient({"failure_events": [failure_row()]}),
        FakeAgent(
            choice=MemoryChoice(
                decision="apply_memory",
                memory_id=RESOLUTION_ID,
                confidence=0.95,
                reason="Same drawer failure.",
            )
        ),
        FakeMemoryStore(index={RESOLUTION_ID: document}),
    )

    match = await service.process_failure(FAILURE_ID)

    assert match is not None
    assert match.actions[0] == {
        "command": "fold",
        "title": "Fold",
        "arguments": {"wait": True},
    }
    rebound = match.actions[-1]
    assert rebound["arguments"]["flow_id"] == "flow-room-101"
    assert rebound["arguments"]["current_action_index"] == 4
    assert (
        rebound["continuation_context"]["expected_arguments"]
        == rebound["arguments"]
    )


@pytest.mark.anyio
async def test_match_rejects_modern_continuation_when_flow_structure_changed(
    tmp_path: Path,
) -> None:
    remembered_arguments = {
        "mode": "rewind",
        "rewind_steps": 1,
        "current_action_index": 4,
        "current_command": "open_drawer",
        "flow_id": "old-flow-run",
    }
    document = memory_document(
        tmp_path,
        actions=(
            {
                "command": "$resume_flow",
                "title": "Rewind",
                "arguments": remembered_arguments,
                "status": "sent",
                "continuation_context": {
                    "expected_arguments": remembered_arguments,
                    "current_action": {
                        "command": "open_drawer",
                        "actionIndex": 4,
                        "areaName": "Closet",
                        "itemName": "Drawer",
                    },
                    "target_action": {
                        "command": "approach_drawer",
                        "actionIndex": 3,
                        "areaName": "Closet",
                        "itemName": "Drawer",
                    },
                },
            },
        ),
    )
    failure = failure_row()
    failure["flow_snapshot"]["areas"][0]["items"][0]["actions"][0][
        "command"
    ] = "approach_cabinet"
    client = FakeClient({"failure_events": [failure]})
    service = processor(
        client,
        FakeAgent(
            choice=MemoryChoice(
                decision="apply_memory",
                memory_id=RESOLUTION_ID,
                confidence=0.95,
                reason="Same drawer failure.",
            )
        ),
        FakeMemoryStore(index={RESOLUTION_ID: document}),
    )

    match = await service.process_failure(FAILURE_ID)

    assert match is None
    row = client.tables["failure_events"][0]
    assert row["matcher_status"] == "no_solution"
    assert row["resolver_suggestion"] is None
    assert "cannot be safely bound" in row["matcher_message"]
    assert "structure differs" in row["matcher_message"]


@pytest.mark.anyio
async def test_agentic_retrieval_searches_reads_and_selects_exact_stored_action(
    tmp_path: Path,
) -> None:
    memory_ids = [
        f"00000000-0000-4000-8000-{index:012d}"
        for index in range(1, 6)
    ]
    target_id = memory_ids[-1]
    documents = {
        memory_id: memory_document(
            tmp_path,
            resolution_id=memory_id,
            failure_text=(
                "The back-room door action was interrupted by a user abort."
                if memory_id == target_id
                else f"Unrelated recovery pattern {memory_id}."
            ),
            actions=(
                {
                    "command": (
                        "fold" if memory_id == target_id else "bump"
                    ),
                    "title": (
                        "Fold" if memory_id == target_id else "Bump"
                    ),
                    "arguments": {"speed": 60},
                    "status": "sent",
                },
            ),
        )
        for memory_id in memory_ids
    }
    agent = FakeAgent(
        retrieval_turns=[
            MemoryRetrievalTurn(
                step=MemoryFinishAction(
                    action="finish",
                    choice=MemoryChoice(
                        decision="no_solution",
                        memory_id=None,
                        confidence=0,
                        reason="Attempted to stop without retrieval.",
                    ),
                )
            ),
            MemoryRetrievalTurn(
                step=MemorySearchAction(
                    action="search",
                    query="back room door interrupted user abort",
                )
            ),
            MemoryRetrievalTurn(
                step=MemoryReadAction(
                    action="read",
                    memory_ids=[target_id],
                )
            ),
            MemoryRetrievalTurn(
                step=MemoryFinishAction(
                    action="finish",
                    choice=MemoryChoice(
                        decision="apply_memory",
                        memory_id=target_id,
                        confidence=0.93,
                        reason="The task, failed step, and abort evidence match.",
                    ),
                )
            ),
        ]
    )
    client = FakeClient({"failure_events": [failure_row()]})
    service = processor(
        client,
        agent,
        FakeMemoryStore(index=documents),
    )

    match = await service.process_failure(FAILURE_ID)

    assert match is not None
    assert match.memory_id == target_id
    assert match.actions == (
        {
            "command": "fold",
            "title": "Fold",
            "arguments": {"speed": 60},
        },
    )
    assert agent.choice_calls == []
    assert len(agent.retrieval_calls) == 4
    assert any(
        observation.get("kind") == "retrieval_correction"
        for observation in agent.retrieval_calls[1][1]
    )
    read_observations = agent.retrieval_calls[-1][1]
    read_markdown = next(
        observation["documents"][0]["markdown"]
        for observation in read_observations
        if observation.get("kind") == "read_results"
        and observation.get("documents")
    )
    assert "back-room door action" in read_markdown
    assert "## Dispatched Actions" not in read_markdown
    assert "must-not-enter-model-prompt" not in read_markdown


@pytest.mark.anyio
async def test_no_memory_finishes_without_calling_model() -> None:
    failure = failure_row()
    failure["resolver_suggestion"] = {
        "memory_id": "stale",
        "summary": "stale",
        "reason": "stale",
        "confidence": 1,
        "actions": [{"command": "stale"}],
    }
    client = FakeClient({"failure_events": [failure]})
    agent = FakeAgent()
    service = processor(client, agent, FakeMemoryStore())

    match = await service.process_failure(FAILURE_ID)

    assert match is None
    assert agent.choice_calls == []
    row = client.tables["failure_events"][0]
    assert row["matcher_status"] == "no_solution"
    assert row["matcher_message"] == (
        "No applicable recovery memory is available."
    )
    assert row["resolver_suggestion"] is None
    assert service.state.no_solution == 1


@pytest.mark.anyio
async def test_memory_with_non_sent_action_is_not_an_execution_candidate(
    tmp_path: Path,
) -> None:
    document = memory_document(
        tmp_path,
        actions=({"command": "bump", "status": "failed"},),
    )
    client = FakeClient({"failure_events": [failure_row()]})
    agent = FakeAgent(
        choice=MemoryChoice(
            decision="apply_memory",
            memory_id=RESOLUTION_ID,
            confidence=1,
            reason="Must not be called.",
        )
    )
    service = processor(
        client,
        agent,
        FakeMemoryStore(index={RESOLUTION_ID: document}),
    )

    match = await service.process_failure(FAILURE_ID)

    assert match is None
    assert agent.choice_calls == []
    assert client.tables["failure_events"][0]["matcher_status"] == "no_solution"
    assert client.tables["failure_events"][0]["resolver_suggestion"] is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "retry_context",
    [
        {},
        {"retried_action": {"actionIndex": 4}},
    ],
)
async def test_rerun_without_retried_action_is_not_suggested(
    tmp_path: Path,
    retry_context: Mapping[str, Any],
) -> None:
    document = memory_document(
        tmp_path,
        actions=(
            {
                "command": "$rerun",
                "title": "Retry",
                "arguments": {},
                "status": "sent",
                "retry_context": retry_context,
            },
        ),
    )
    client = FakeClient({"failure_events": [failure_row()]})
    agent = FakeAgent(
        choice=MemoryChoice(
            decision="apply_memory",
            memory_id=RESOLUTION_ID,
            confidence=1,
            reason="Must not be called.",
        )
    )
    service = processor(
        client,
        agent,
        FakeMemoryStore(index={RESOLUTION_ID: document}),
    )

    match = await service.process_failure(FAILURE_ID)

    assert match is None
    assert agent.choice_calls == []
    assert client.tables["failure_events"][0]["matcher_status"] == "no_solution"
    assert client.tables["failure_events"][0]["resolver_suggestion"] is None


@pytest.mark.anyio
async def test_unprovided_or_non_candidate_memory_id_fails_closed(
    tmp_path: Path,
) -> None:
    document = memory_document(
        tmp_path,
        actions=(
            {
                "command": "$rerun",
                "status": "sent",
                "retry_context": {
                    "retried_action": {"command": "open_drawer"}
                },
            },
        ),
    )
    agent = FakeAgent(
        choice=MemoryChoice(
            decision="apply_memory",
            memory_id="cb6f575f-cf48-4db4-99c8-2a374f0efca9",
            confidence=1,
            reason="Invented selection.",
        )
    )
    client = FakeClient({"failure_events": [failure_row()]})
    service = processor(
        client,
        agent,
        FakeMemoryStore(index={RESOLUTION_ID: document}),
    )

    with pytest.raises(
        AgentReasoningError,
        match="not an execution candidate",
    ):
        await service.process_failure(FAILURE_ID)

    row = client.tables["failure_events"][0]
    assert row["matcher_status"] == "failed"
    assert row["matcher_message"] == (
        "The solution matcher could not complete safely."
    )
    assert row["resolver_suggestion"] is None
    assert service.state.match_errors == 1


@pytest.mark.anyio
async def test_site_zero_memory_routes_to_dev_store() -> None:
    # resolution_row/failure fixtures are site_id 0 — the dev site.
    client = FakeClient({"flow_failure_resolutions": [resolution_row()]})
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    dev_store = FakeMemoryStore()
    service = processor(client, agent, store, dev_store)

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is not None
    assert store.writes == []
    assert len(dev_store.writes) == 1


@pytest.mark.anyio
async def test_production_site_memory_routes_to_main_store() -> None:
    row = resolution_row()
    row["site_id"] = 7
    client = FakeClient({"flow_failure_resolutions": [row]})
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    dev_store = FakeMemoryStore()
    service = processor(client, agent, store, dev_store)

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is not None
    assert len(store.writes) == 1
    assert dev_store.writes == []


@pytest.mark.anyio
async def test_resolved_applied_episode_writes_positive_markdown_draft() -> None:
    raw_row = resolution_row()
    client = FakeClient({"flow_failure_resolutions": [raw_row]})
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    service = processor(client, agent, store)

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is not None
    assert result.commit_sha == "abc123"
    assert len(agent.generalization_calls) == 1
    generalized_row = agent.generalization_calls[0]
    assert [action["command"] for action in generalized_row["action_runs"]] == [
        "$rerun"
    ]
    assert len(client.tables["flow_failure_resolutions"][0]["action_runs"]) == 2

    assert len(store.writes) == 1
    draft = store.writes[0]
    assert draft.memory_kind == "positive"
    assert draft.actionable is True
    assert draft.source.payload["site"] == "Office"
    assert draft.source.payload["room_number"] == "101"
    assert draft.source.payload["navigation"]["current_map"]["id"] == 39
    assert [action["command"] for action in draft.source.dispatched_actions] == [
        "$rerun"
    ]
    assert draft.failure_summary.startswith("The drawer-opening")
    assert draft.signature == {
        "task_family": "open drawer",
        "failed_step": "open drawer",
        "failure_mode": "already open",
        "object_state": "open",
        "context": ["closet"],
    }
    assert draft.lessons == (
        "Retrieval tag: drawer",
        "Retrieval tag: already-open",
    )
    assert service.state.memories_written == 1
    assert service.state.last_memory_commit == "abc123"


@pytest.mark.anyio
async def test_linked_failure_episode_is_merged_and_retained_for_memory() -> None:
    resolution = resolution_row()
    resolution.update(
        {
            "site": "Resolution-owned site",
            "flow_id": None,
            "flow_name": None,
            "failure_status": None,
            "failure_reason": None,
            "failed_command": None,
            "failed_action": None,
            "action_runs": [
                {
                    "command": "fold",
                    "title": "Fold",
                    "arguments": {"speed": 60},
                    "status": "sent",
                    "state_before": {"arm": {"joints": [1, 2, 3]}},
                },
                {
                    "command": "bump",
                    "title": "Bump",
                    "arguments": {},
                    "status": "failed",
                },
            ],
        }
    )
    failure = failure_row()
    failure["sanitized_context"]["location"] = {
        "site_id": 0,
        "site": "Office",
        "floor": "ground",
        "room_number": "101",
        "map_id": 39,
        "map_name": "BILLIE-17-OFFICE",
    }
    failure.update(
        {
            "flow_id": "flow-open-door",
            "flow_name": "Open Door For Testing",
            "flow_status": "paused",
            "action_command": "replay_policy",
            "description": "The door-opening action was user-aborted.",
            "failed_step": "Open door in back room",
            "robot_errors": [
                {
                    "reported_at": "2026-07-28T19:27:53Z",
                    "message": "Action interrupted.",
                }
            ],
            "flow_snapshot": {
                "steps": [
                    {"title": "Travel to back room", "status": "completed"},
                    {"title": "Open door in back room", "status": "paused"},
                ]
            },
            "operator_email": "must-not-enter-memory@example.com",
            "input_tokens": 321,
            "matcher_message": "mutable resolver bookkeeping",
            "transport": {
                "access_token": "must-not-enter-memory",
                "connected": True,
            },
        }
    )
    client = FakeClient(
        {
            "failure_events": [failure],
            "flow_failure_resolutions": [resolution],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    service = processor(client, agent, store)

    await service.learn_resolution(RESOLUTION_ID)

    model_row = agent.generalization_calls[0]
    assert model_row["site"] == "Resolution-owned site"
    assert model_row["flow_id"] == "flow-open-door"
    assert model_row["flow_name"] == "Open Door For Testing"
    assert model_row["failure_status"] == "paused"
    assert model_row["failure_reason"] == (
        "The door-opening action was user-aborted."
    )
    assert model_row["failed_command"] == "replay_policy"
    assert model_row["failed_action"] == "Open door in back room"
    assert model_row["failed_step"] == "Open door in back room"
    assert model_row["floor"] == "ground"
    assert [run["command"] for run in model_row["action_runs"]] == ["fold"]

    evidence = model_row["episode_evidence"]
    assert evidence["failure_event"]["robot_errors"][0]["message"] == (
        "Action interrupted."
    )
    assert evidence["failure_event"]["flow_snapshot"]["steps"][1]["title"] == (
        "Open door in back room"
    )
    assert "operator_email" not in evidence["failure_event"]
    assert evidence["failure_event"]["input_tokens"] == 321
    assert "matcher_message" not in evidence["failure_event"]
    assert evidence["failure_event"]["transport"] == {"connected": True}
    assert len(evidence["resolution_event"]["action_runs"]) == 2
    assert evidence["resolution_event"]["outcome"] == "resolved"
    assert evidence["resolution_event"]["applied"] is True

    source_payload = store.writes[0].source.payload
    assert source_payload["episode_evidence"] == evidence
    assert source_payload["flow_name"] == "Open Door For Testing"
    assert source_payload["failed_step"] == "Open door in back room"
    assert [run["command"] for run in store.writes[0].source.dispatched_actions] == [
        "fold"
    ]


@pytest.mark.anyio
async def test_linked_failure_change_invalidates_resolution_source_hash() -> None:
    hashes: list[str] = []
    for description in ("Door was blocked.", "Door was already open."):
        failure = failure_row()
        failure["description"] = description
        resolution = resolution_row()
        resolution["failure_reason"] = None
        store = FakeMemoryStore()
        service = processor(
            FakeClient(
                {
                    "failure_events": [failure],
                    "flow_failure_resolutions": [resolution],
                }
            ),
            FakeAgent(generalization=generalization()),
            store,
        )
        await service.learn_resolution(RESOLUTION_ID)
        hashes.append(store.writes[0].source.source_hash)

    assert hashes[0] != hashes[1]


@pytest.mark.anyio
async def test_ingested_resolution_is_rebuilt_when_source_hash_changed() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "ingested",
            "memory_resolution_id": RESOLUTION_ID,
            "memory_commit_sha": "old-commit",
        }
    )
    store = FakeMemoryStore(has_source_hash=False)
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    service = processor(
        client,
        FakeAgent(generalization=generalization()),
        store,
    )

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is not None
    assert len(store.writes) == 1
    assert store.writes[0].source.payload["episode_evidence"][
        "failure_event"
    ]["flow_name"] == "Open room drawer"
    assert [update[1]["memory_status"] for update in client.updates] == [
        "ingested"
    ]
    row = client.tables["failure_events"][0]
    assert row["memory_status"] == "ingested"
    assert row["memory_commit_sha"] == "abc123"
    assert row["memory_message"] == "Recovery memory ingested."
    assert datetime.fromisoformat(row["memory_ingested_at"])
    assert client.updates[0][2] == [
        ("failure_id", FAILURE_ID),
        ("memory_status", "ingested"),
        ("memory_resolution_id", RESOLUTION_ID),
        ("memory_commit_sha", "old-commit"),
    ]


@pytest.mark.anyio
async def test_unchanged_ingested_memory_refreshes_stale_commit_ack() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "ingested",
            "memory_resolution_id": RESOLUTION_ID,
            "memory_commit_sha": "existing-commit",
            "memory_message": "Recovery memory ingested.",
            "memory_ingested_at": "2026-07-28T18:10:00+00:00",
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    service = processor(
        client,
        FakeAgent(generalization=generalization()),
        FakeMemoryStore(has_source_hash=True),
    )

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is None
    assert [update[1]["memory_status"] for update in client.updates] == [
        "ingested"
    ]
    row = client.tables["failure_events"][0]
    assert row["memory_status"] == "ingested"
    assert row["memory_commit_sha"] == "f" * 40
    assert datetime.fromisoformat(row["memory_ingested_at"])


@pytest.mark.anyio
async def test_unchanged_ingested_memory_preserves_current_commit_ack() -> None:
    current_commit = "e" * 40
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "ingested",
            "memory_resolution_id": RESOLUTION_ID,
            "memory_commit_sha": current_commit,
            "memory_message": "Recovery memory ingested.",
            "memory_ingested_at": "2026-07-28T18:10:00+00:00",
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    service = processor(
        client,
        FakeAgent(generalization=generalization()),
        FakeMemoryStore(
            has_source_hash=True,
            latest_commit=current_commit,
        ),
    )

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is None
    assert client.updates == []
    row = client.tables["failure_events"][0]
    assert row["memory_commit_sha"] == current_commit
    assert row["memory_ingested_at"] == "2026-07-28T18:10:00+00:00"


@pytest.mark.anyio
async def test_linked_memory_ingestion_is_acknowledged_atomically() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "matcher_status": "no_solution",
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    service = processor(
        client,
        FakeAgent(generalization=generalization()),
        FakeMemoryStore(),
    )

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is not None
    assert [update[1]["memory_status"] for update in client.updates] == [
        "ingesting",
        "ingested",
    ]
    row = client.tables["failure_events"][0]
    assert row["matcher_status"] == "no_solution"
    assert row["memory_status"] == "ingested"
    assert row["memory_resolution_id"] == RESOLUTION_ID
    assert row["memory_commit_sha"] == "abc123"
    assert row["memory_message"] == "Recovery memory ingested."
    assert datetime.fromisoformat(row["memory_ingested_at"])
    assert service.state.memory_acks_ingested == 1


@pytest.mark.anyio
async def test_linked_memory_ingestion_failure_is_acknowledged_safely() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    service = processor(
        client,
        FakeAgent(
            generalization=generalization(),
            generalization_error=RuntimeError("secret model failure"),
        ),
        FakeMemoryStore(),
    )

    with pytest.raises(RuntimeError, match="secret model failure"):
        await service.learn_resolution(RESOLUTION_ID)

    row = client.tables["failure_events"][0]
    assert row["memory_status"] == "failed"
    assert row["memory_resolution_id"] == RESOLUTION_ID
    assert row["memory_commit_sha"] is None
    assert row["memory_message"] == "Recovery memory ingestion failed."
    assert "secret" not in row["memory_message"]
    assert row["memory_ingested_at"] is None
    assert service.state.memory_acks_failed == 1


@pytest.mark.anyio
async def test_failed_memory_ingestion_is_retryable_on_later_resolution_event() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    agent = FakeAgent(
        generalization=generalization(),
        generalization_error=RuntimeError("transient model failure"),
    )
    store = FakeMemoryStore()
    service = processor(client, agent, store)

    with pytest.raises(RuntimeError, match="transient model failure"):
        await service.learn_resolution(RESOLUTION_ID)

    assert client.tables["failure_events"][0]["memory_status"] == "failed"

    agent.generalization_error = None
    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is not None
    assert result.commit_sha == "abc123"
    assert [
        update[1]["memory_status"]
        for update in client.updates
        if update[0] == "failure_events"
    ] == ["ingesting", "failed", "ingesting", "ingested"]
    retry_claim = client.updates[2]
    assert ("memory_status", "failed") in retry_claim[2]
    row = client.tables["failure_events"][0]
    assert row["memory_status"] == "ingested"
    assert row["memory_commit_sha"] == "abc123"
    assert service.state.memory_acks_failed == 1
    assert service.state.memory_acks_ingested == 1


@pytest.mark.anyio
async def test_ambiguous_rerun_is_not_written_as_actionable_memory() -> None:
    row = resolution_row()
    row["action_runs"] = [
        {
            "command": "$rerun",
            "title": "Retry",
            "arguments": {},
            "status": "sent",
            "retry_context": {},
        }
    ]
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [row],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    service = processor(client, agent, store)

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is None
    assert agent.generalization_calls == []
    assert store.writes == []
    assert client.updates == []
    assert client.tables["failure_events"][0]["memory_status"] == "pending"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "patch",
    [
        {"outcome": "recorded"},
        {"outcome": "unresolved"},
        {"applied": False},
        {
            "action_runs": [
                {"command": "bump", "arguments": {}, "status": "failed"}
            ]
        },
    ],
)
async def test_only_resolved_applied_sent_actions_are_learned(patch) -> None:
    row = resolution_row()
    row.update(patch)
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [row],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    service = processor(client, agent, store)

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is None
    assert agent.generalization_calls == []
    assert store.hash_calls == []
    assert store.writes == []
    assert service.state.resolutions_skipped == 1
    assert client.updates == []


@pytest.mark.anyio
async def test_existing_source_hash_skips_model_and_git_write() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore(has_source_hash=True)
    service = processor(client, agent, store)

    result = await service.learn_resolution(RESOLUTION_ID)

    assert result is None
    assert len(store.hash_calls) == 1
    assert agent.generalization_calls == []
    assert store.writes == []
    assert service.state.resolutions_skipped == 1
    row = client.tables["failure_events"][0]
    assert row["memory_status"] == "ingested"
    assert row["memory_commit_sha"] == "f" * 40
    assert row["memory_message"] == "Recovery memory ingested."


@pytest.mark.anyio
async def test_runtime_subscribes_to_both_tables_and_reconciles_changes() -> None:
    client = FakeClient(
        {
            "failure_events": [],
            "flow_failure_resolutions": [],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()

    async def client_factory(url: str, key: str):
        return client

    runtime = SupabaseAgentRuntime(
        settings(),
        agent=agent,
        memory_store=store,
        client_factory=client_factory,
    )
    task = asyncio.create_task(runtime.run())
    await wait_until(lambda: runtime.state.connected)

    tables = {
        binding["table"]
        for channel in client.channels.values()
        for binding in channel.bindings.values()
    }
    assert tables == {"failure_events", "flow_failure_resolutions"}

    pending_reconciliation_query = next(
        query
        for query in client.queries
        if query.table == "failure_events"
        and query.columns == "failure_id"
        and ("matcher_status", "pending") in query.filters
    )
    assert (
        "analysis_status",
        "completed",
    ) in pending_reconciliation_query.filters

    analyzing_failure = failure_row()
    analyzing_failure["analysis_status"] = "analyzing"
    client.tables["failure_events"].append(analyzing_failure)
    failure_channel = next(
        channel
        for channel in client.channels.values()
        if any(
            binding["table"] == "failure_events"
            for binding in channel.bindings.values()
        )
    )
    failure_channel.emit("INSERT", {"failure_id": FAILURE_ID})
    await wait_until(
        lambda: runtime.processor.state.failures_skipped == 1
    )
    assert client.tables["failure_events"][0]["matcher_status"] == "pending"
    assert store.index_calls == 0

    client.tables["failure_events"][0]["analysis_status"] = "completed"
    failure_channel.emit("UPDATE", {"failure_id": FAILURE_ID})
    await wait_until(
        lambda: (
            client.tables["failure_events"][0]["matcher_status"]
            == "no_solution"
        )
    )

    client.tables["flow_failure_resolutions"].append(resolution_row())
    client.tables["failure_events"][0].update(
        {
            "memory_status": "pending",
            "memory_resolution_id": RESOLUTION_ID,
        }
    )
    failure_channel.emit("UPDATE", {"failure_id": FAILURE_ID})
    await wait_until(
        lambda: (
            client.tables["failure_events"][0]["memory_status"] == "ingested"
        )
    )

    assert runtime.processor.state.no_solution == 1
    assert runtime.processor.state.memories_written == 1
    assert runtime.processor.state.memory_acks_ingested == 1

    await runtime.stop()
    await task
    assert agent.close_calls == 1


@pytest.mark.anyio
async def test_runtime_starts_reconciles_notifies_and_stops_auto_coordinator(
) -> None:
    client = FakeClient(
        {
            "failure_events": [],
            "flow_failure_resolutions": [],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore()
    recovery = FakeRecoveryCoordinator()

    async def client_factory(url: str, key: str):
        return client

    runtime = SupabaseAgentRuntime(
        settings(
            auto_execute=True,
            recovery_robot_allowlist=("BILLIE-16",),
            recovery_cf_access_client_id="client-id",
            recovery_cf_access_client_secret="client-secret",
        ),
        agent=agent,
        memory_store=store,
        client_factory=client_factory,
        recovery_coordinator=recovery,  # type: ignore[arg-type]
    )
    task = asyncio.create_task(runtime.run())
    await wait_until(lambda: runtime.state.connected)

    assert recovery.start_calls == 1
    assert recovery.reconcile_calls == 1
    assert recovery.databases[-1] is not None

    analyzing = failure_row()
    analyzing["analysis_status"] = "analyzing"
    client.tables["failure_events"].append(analyzing)
    failure_channel = next(
        channel
        for channel in client.channels.values()
        if any(
            binding["table"] == "failure_events"
            for binding in channel.bindings.values()
        )
    )
    failure_channel.emit("INSERT", {"failure_id": FAILURE_ID})
    await wait_until(
        lambda: recovery.failure_ids == [FAILURE_ID, FAILURE_ID]
    )

    snapshot = runtime.snapshot()
    assert snapshot["auto_recovery_enabled"] is True

    await runtime.stop()
    await task

    assert recovery.stop_calls == 1
    assert recovery.databases[-1] is None


@pytest.mark.anyio
async def test_startup_reconciliation_recovers_failed_memory_ingestion() -> None:
    linked_failure = failure_row()
    linked_failure.update(
        {
            "matcher_status": "no_solution",
            "memory_status": "failed",
            "memory_resolution_id": RESOLUTION_ID,
            "memory_message": "Recovery memory ingestion failed.",
        }
    )
    client = FakeClient(
        {
            "failure_events": [linked_failure],
            "flow_failure_resolutions": [resolution_row()],
        }
    )
    agent = FakeAgent(generalization=generalization())
    store = FakeMemoryStore(has_source_hash=True)

    async def client_factory(url: str, key: str):
        return client

    runtime = SupabaseAgentRuntime(
        settings(),
        agent=agent,
        memory_store=store,
        client_factory=client_factory,
    )
    task = asyncio.create_task(runtime.run())
    await wait_until(
        lambda: (
            client.tables["failure_events"][0]["memory_status"] == "ingested"
        )
    )

    memory_updates = [
        update
        for update in client.updates
        if update[0] == "failure_events"
        and "memory_status" in update[1]
    ]
    assert [update[1]["memory_status"] for update in memory_updates] == [
        "ingesting",
        "ingested",
    ]
    assert ("memory_status", "failed") in memory_updates[0][2]
    assert agent.generalization_calls == []
    assert store.writes == []
    assert runtime.processor.state.memory_acks_ingested == 1

    await runtime.stop()
    await task
