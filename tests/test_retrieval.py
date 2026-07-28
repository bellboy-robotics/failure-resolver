from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pytest

from memory_store import MemoryDocument
from retrieval import (
    MAX_MEMORY_READ_CHARS,
    MAX_QUERY_CHARS,
    MAX_READ_DOCUMENTS,
    MAX_SEARCH_RESULTS,
    MemoryRetrievalIndex,
    RetrievalBudgetError,
    RetrievalError,
    UnknownMemoryError,
)


def memory_document(
    tmp_path: Path,
    memory_id: str,
    *,
    frontmatter: Mapping[str, Any] | None = None,
    prose: str = "A manipulation step failed.",
    signature: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Any] | None = None,
    dispatched_command: str = "trusted-command",
    actionable: bool = True,
) -> MemoryDocument:
    metadata = {
        "resolution_id": memory_id,
        "source_hash": memory_id[0] * 64,
        "memory_kind": "positive",
        "actionable": actionable,
        "outcome": "resolved",
        "applied": True,
        **(frontmatter or {}),
    }
    body = (
        f"# Memory {memory_id}\n\n"
        "## Failure Pattern\n"
        f"> {prose}\n\n"
        "## Recovery Knowledge\n"
        "> Use the demonstrated recovery when the preconditions match.\n\n"
        "## Retrieval Signature\n"
        "```json\n"
        '{"source":"parsed-structured-field"}\n'
        "```\n\n"
        "## Episode Evidence\n"
        "```json\n"
        '{"source":"parsed-structured-field"}\n'
        "```\n\n"
        "## Dispatched Actions\n"
        "```json\n"
        f'[{{"command":"{dispatched_command}","arguments":{{"force":7}}}}]\n'
        "```\n"
    )
    return MemoryDocument(
        path=tmp_path / f"{memory_id}.md",
        frontmatter=metadata,
        dispatched_actions=(
            {
                "command": dispatched_command,
                "arguments": {"force": 7},
                "status": "sent",
            },
        ),
        body=body,
        episode_evidence=evidence,
        retrieval_signature=signature,
    )


def test_weighted_search_is_deterministic_and_signature_ranks_first(
    tmp_path: Path,
) -> None:
    signature_match = memory_document(
        tmp_path,
        "a-signature",
        signature={"failure_mode": "reader flashes crimson"},
    )
    prose_match = memory_document(
        tmp_path,
        "b-prose",
        prose="The room-card reader flashes crimson.",
    )
    evidence_match = memory_document(
        tmp_path,
        "c-evidence",
        evidence={"robot_error": "reader flashes crimson"},
    )
    no_match = memory_document(
        tmp_path,
        "d-no-match",
        prose="The drawer was obstructed.",
    )
    index = MemoryRetrievalIndex(
        [no_match, evidence_match, prose_match, signature_match]
    )

    first = index.search("reader flashes crimson", limit=4)
    second = index.search("reader flashes crimson", limit=4)

    assert first == second
    assert [result.memory_id for result in first] == [
        "a-signature",
        "b-prose",
        "c-evidence",
        "d-no-match",
    ]
    assert first[0].matched_fields == ("retrieval_signature",)
    assert first[1].matched_fields == ("generalized_prose",)
    assert first[2].matched_fields == ("episode_evidence",)
    assert first[3].score == 0


def test_metadata_hints_boost_but_do_not_filter_results(
    tmp_path: Path,
) -> None:
    other_flow = memory_document(
        tmp_path,
        "a-other",
        frontmatter={"flow_id": "flow-other", "room_number": "101"},
        prose="Door opening was interrupted.",
    )
    hinted_flow = memory_document(
        tmp_path,
        "b-hinted",
        frontmatter={"flow_id": "flow-target", "room_number": "202"},
        prose="Door opening was interrupted.",
    )
    index = MemoryRetrievalIndex([other_flow, hinted_flow])

    results = index.search(
        "door opening interrupted",
        hints={"flow_id": "flow-target"},
    )

    assert [result.memory_id for result in results] == [
        "b-hinted",
        "a-other",
    ]
    assert all(result.score > 0 for result in results)

    # Missing failure metadata must not favor memories that also omit it.
    without_missing_hint_bias = index.search(
        "door opening interrupted",
        hints={"flow_id": None},
    )
    assert [result.memory_id for result in without_missing_hint_bias] == [
        "a-other",
        "b-hinted",
    ]


def test_read_contains_all_searchable_data_and_only_omits_action_section(
    tmp_path: Path,
) -> None:
    document = memory_document(
        tmp_path,
        "a-complete",
        frontmatter={
            "site": "Bellboy Hotel",
            "room_number": "814",
            "map_name": "floor-eight",
            "flow_name": "Deliver room card",
        },
        prose="The card reader emitted a short red flash.",
        signature={
            "task_family": "unlock room",
            "failure_mode": "red reader flash",
        },
        evidence={
            "failure_event": {
                "robot_errors": [
                    {
                        "reported_at": "2026-07-28T19:27:53Z",
                        "message": "Access was denied.",
                    }
                ]
            },
            "resolution_event": {
                "description": "Operator selected fold.",
                "action_runs": [
                    {
                        "command": "observed-fold",
                        "arguments": {"speed": "slow"},
                    }
                ],
            },
        },
        dispatched_command="trusted-executable-fold",
    )
    index = MemoryRetrievalIndex([document])

    result = index.search("Access was denied", limit=1)[0]
    read = index.read([result.memory_id])[0]

    assert result.matched_fields == ("episode_evidence",)
    assert "Bellboy Hotel" in read.markdown
    assert "floor-eight" in read.markdown
    assert "short red flash" in read.markdown
    assert "red reader flash" in read.markdown
    assert "2026-07-28T19:27:53Z" in read.markdown
    # Complete episode evidence is searchable, including observed action data.
    assert "observed-fold" in read.markdown
    assert '"speed": "slow"' in read.markdown
    # The structurally authoritative payload is never copied into model reads.
    assert "## Dispatched Actions" not in read.markdown
    assert "trusted-executable-fold" not in read.markdown
    assert '"force": 7' not in read.markdown


def test_snapshot_does_not_change_when_source_mappings_mutate(
    tmp_path: Path,
) -> None:
    frontmatter = {"flow_name": "Original flow"}
    evidence = {"failure_event": {"description": "Original evidence"}}
    document = memory_document(
        tmp_path,
        "a-immutable",
        frontmatter=frontmatter,
        evidence=evidence,
    )
    index = MemoryRetrievalIndex([document])

    document.frontmatter["flow_name"] = "Mutated flow"
    evidence["failure_event"]["description"] = "Mutated evidence"

    read = index.read(["a-immutable"])[0]
    assert "Original flow" in read.markdown
    assert "Original evidence" in read.markdown
    assert "Mutated flow" not in read.markdown
    assert "Mutated evidence" not in read.markdown
    read.metadata["flow_name"] = "Cannot mutate"
    assert (
        index.read(["a-immutable"])[0].metadata["flow_name"]
        == "Original flow"
    )


def test_non_actionable_documents_are_not_in_snapshot(
    tmp_path: Path,
) -> None:
    eligible = memory_document(tmp_path, "a-eligible")
    negative = memory_document(
        tmp_path,
        "b-negative",
        actionable=False,
        frontmatter={"memory_kind": "negative"},
    )

    index = MemoryRetrievalIndex([negative, eligible])

    assert index.memory_ids == ("a-eligible",)


def test_read_requires_exact_known_unique_ids_and_preserves_order(
    tmp_path: Path,
) -> None:
    index = MemoryRetrievalIndex(
        [
            memory_document(tmp_path, "a-first"),
            memory_document(tmp_path, "b-second"),
        ]
    )

    assert [
        result.memory_id
        for result in index.read(["b-second", "a-first"])
    ] == ["b-second", "a-first"]
    with pytest.raises(UnknownMemoryError, match="invented"):
        index.read(["invented"])
    with pytest.raises(RetrievalError, match="duplicated"):
        index.read(["a-first", "a-first"])


def test_hard_search_and_read_bounds_are_enforced(tmp_path: Path) -> None:
    documents = [
        memory_document(tmp_path, f"{letter}-memory")
        for letter in "abcdefghi"
    ]
    index = MemoryRetrievalIndex(documents)

    with pytest.raises(RetrievalBudgetError, match="index.*document"):
        MemoryRetrievalIndex(documents[:2], max_documents=1)
    with pytest.raises(RetrievalBudgetError, match="index.*character"):
        MemoryRetrievalIndex(documents[:1], max_chars=1)
    with pytest.raises(RetrievalBudgetError, match="result limit"):
        index.search("failure", limit=MAX_SEARCH_RESULTS + 1)
    with pytest.raises(RetrievalBudgetError, match="length"):
        index.search("x" * (MAX_QUERY_CHARS + 1))
    with pytest.raises(RetrievalBudgetError, match="document budget"):
        index.read(list(index.memory_ids[: MAX_READ_DOCUMENTS + 1]))
    with pytest.raises(RetrievalBudgetError, match="character budget"):
        index.read(["a-memory"], max_total_chars=1)


def test_auto_read_only_returns_all_when_complete_snapshot_fits(
    tmp_path: Path,
) -> None:
    small = MemoryRetrievalIndex(
        [
            memory_document(tmp_path, "a-small"),
            memory_document(tmp_path, "b-small"),
        ]
    )
    eligible = small.auto_read_eligibility()

    assert eligible.eligible is True
    assert eligible.memory_ids == ("a-small", "b-small")
    assert eligible.total_chars == sum(
        result.char_count
        for result in small.read(eligible.memory_ids)
    )
    assert small.auto_read_ids() == eligible.memory_ids

    too_many = MemoryRetrievalIndex(
        [
            memory_document(tmp_path, f"{letter}-many")
            for letter in "abcde"
        ]
    )
    ineligible = too_many.auto_read_eligibility()
    assert ineligible.eligible is False
    assert ineligible.memory_ids == ()
    assert ineligible.reason == "too_many_documents"
    assert too_many.auto_read_ids() == ()


def test_oversized_memory_fails_closed_for_read_and_auto_read(
    tmp_path: Path,
) -> None:
    document = memory_document(
        tmp_path,
        "a-large",
        prose="x" * (MAX_MEMORY_READ_CHARS + 1),
    )
    index = MemoryRetrievalIndex([document])

    with pytest.raises(RetrievalBudgetError, match="per-memory"):
        index.read(["a-large"])
    eligibility = index.auto_read_eligibility()
    assert eligibility.eligible is False
    assert eligibility.memory_ids == ()
    assert eligibility.reason == "content_too_large"
