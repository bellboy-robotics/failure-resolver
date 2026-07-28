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
    ResolutionGeneralization,
)
from memory_store import MemoryDocument, MemoryWriteResult
from resolver import (
    FailureResolverProcessor,
    ResolverSettings,
    SupabaseAgentRuntime,
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
        self.limit_value: int | None = None
        self.order_column: str | None = None
        self.update_values: dict[str, Any] | None = None

    def select(self, columns: str) -> "FakeQuery":
        self.columns = columns
        return self

    def eq(self, column: str, value: Any) -> "FakeQuery":
        self.filters.append((column, value))
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
        generalization: ResolutionGeneralization | None = None,
        generalization_error: Exception | None = None,
    ) -> None:
        self.choice = choice
        self.generalization = generalization
        self.generalization_error = generalization_error
        self.choice_calls: list[tuple[Mapping[str, Any], Any]] = []
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
    ) -> None:
        self.index = dict(index or {})
        self.has_source_hash_result = has_source_hash
        self.index_calls = 0
        self.hash_calls: list[tuple[str, str, bool]] = []
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

    async def awrite_memory(self, draft):
        self.writes.append(draft)
        return MemoryWriteResult(
            resolution_id=draft.source.resolution_id,
            relative_path=f"memories/{draft.source.resolution_id}.md",
            changed=True,
            commit_sha="abc123",
        )


def failure_row() -> dict[str, Any]:
    return {
        "failure_id": FAILURE_ID,
        "sysid": "BILLIE-16",
        "incident_key": "incident-1",
        "flow_id": "flow-room-101",
        "flow_name": "Open room drawer",
        "flow_status": "error",
        "failure_kind": "action_failed",
        "action_command": "open_drawer",
        "description": "The drawer was already open.",
        "failed_step": "Open drawer",
        "reported_cause": "drawer already open",
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
        "sanitized_context": {
            "flow": {"site": "Office", "room": "101"},
            "action": {"area_name": "Closet", "item_name": "Drawer"},
        },
        "created_at": "2026-07-28T18:00:00Z",
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
                    }
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
) -> MemoryDocument:
    path = tmp_path / f"{RESOLUTION_ID}.md"
    body = (
        "# Drawer memory\n\n"
        "## Failure Pattern\n"
        "> The drawer was already open.\n\n"
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
            "resolution_id": RESOLUTION_ID,
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
) -> FailureResolverProcessor:
    return FailureResolverProcessor(
        failure_events_table="failure_events",
        resolutions_table="flow_failure_resolutions",
        agent=agent,
        memory_store=store,
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
                }
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
                }
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
    assert draft.lessons == (
        "Retrieval tag: drawer",
        "Retrieval tag: already-open",
    )
    assert service.state.memories_written == 1
    assert service.state.last_memory_commit == "abc123"


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
    assert row["memory_commit_sha"] is None
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

    client.tables["failure_events"].append(failure_row())
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
