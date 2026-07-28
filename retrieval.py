"""Deterministic, action-safe retrieval over Git-backed memories.

The snapshot in this module is deliberately local and model-free.  It excludes
the structural ``## Dispatched Actions`` section from model reads.  Complete
episode evidence remains searchable, including historical action observations;
trusted application code must still source any executable recovery from
``MemoryDocument.dispatched_actions`` rather than model-produced text.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from memory_store import MemoryDocument


MAX_SEARCH_CALLS = 4
MAX_SEARCH_RESULTS = 8
MAX_READ_DOCUMENTS = 4
MAX_TOTAL_READ_CHARS = 300_000
MAX_MEMORY_READ_CHARS = 200_000
MAX_INDEX_DOCUMENTS = 2_000
MAX_INDEX_CHARS = 64_000_000
MAX_QUERY_CHARS = 500
MAX_SEARCH_HINTS = 16
MAX_HINT_CHARS = 500
MAX_SNIPPET_CHARS = 800

_FIELD_WEIGHTS = MappingProxyType(
    {
        "retrieval_signature": 12,
        "frontmatter": 8,
        "generalized_prose": 6,
        "episode_evidence": 4,
    }
)
_FIELD_ORDER = tuple(_FIELD_WEIGHTS)
_HINT_EXACT_BOOST = 50
_HINT_TOKEN_BOOST = 8
_PHRASE_MULTIPLIER = 3
_MAX_TERM_FREQUENCY = 4
_TOKEN_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)
_SECTION_PATTERN = re.compile(r"(?m)^## ([^\n]+)\n")


class RetrievalError(ValueError):
    """Base error for invalid or unsafe retrieval operations."""


class RetrievalBudgetError(RetrievalError):
    """The requested operation exceeds a hard retrieval bound."""


class UnknownMemoryError(RetrievalError):
    """A read requested an ID outside the immutable snapshot."""


@dataclass(frozen=True)
class MemorySearchResult:
    memory_id: str
    score: int
    matched_fields: tuple[str, ...]
    snippet: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class MemoryReadResult:
    memory_id: str
    markdown: str
    char_count: int
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class AutoReadEligibility:
    eligible: bool
    memory_ids: tuple[str, ...]
    total_chars: int
    reason: str


@dataclass(frozen=True)
class _SearchField:
    name: str
    text: str
    normalized: str
    term_counts: Mapping[str, int]


@dataclass(frozen=True)
class _IndexedMemory:
    memory_id: str
    markdown: str
    metadata: Mapping[str, Any]
    fields: tuple[_SearchField, ...]


class MemoryRetrievalIndex:
    """An immutable, deterministic search snapshot.

    ``documents`` should already be restricted to safe execution candidates by
    the caller.  This module additionally drops non-actionable documents so an
    accidental unfiltered index cannot make negative memory readable as a
    recovery candidate.
    """

    def __init__(
        self,
        documents: Mapping[str, MemoryDocument] | Iterable[MemoryDocument],
        *,
        max_documents: int = MAX_INDEX_DOCUMENTS,
        max_chars: int = MAX_INDEX_CHARS,
    ) -> None:
        if isinstance(documents, Mapping):
            supplied = tuple(documents.items())
        else:
            supplied = tuple(
                (document.resolution_id, document)
                for document in documents
            )
        document_budget = _validated_index_document_budget(max_documents)
        character_budget = _validated_index_character_budget(max_chars)
        if len(supplied) > document_budget:
            raise RetrievalBudgetError(
                "memory index exceeds the document budget"
            )

        by_id: dict[str, MemoryDocument] = {}
        indexed_chars = 0
        for supplied_id, document in supplied:
            if not isinstance(document, MemoryDocument):
                raise RetrievalError(
                    "retrieval documents must be MemoryDocument objects"
                )
            memory_id = document.resolution_id
            if str(supplied_id) != memory_id:
                raise RetrievalError(
                    "retrieval mapping key must match resolution_id"
                )
            if memory_id in by_id:
                raise RetrievalError("retrieval memory IDs must be unique")
            if document.is_execution_candidate:
                indexed_chars += _document_index_chars(document)
                if indexed_chars > character_budget:
                    raise RetrievalBudgetError(
                        "memory index exceeds the character budget"
                    )
                by_id[memory_id] = document

        entries = tuple(
            _index_document(by_id[memory_id])
            for memory_id in sorted(by_id)
        )
        self._entries = entries
        self._by_id = MappingProxyType(
            {entry.memory_id: entry for entry in entries}
        )

    @property
    def memory_ids(self) -> tuple[str, ...]:
        return tuple(entry.memory_id for entry in self._entries)

    @property
    def document_count(self) -> int:
        return len(self._entries)

    def search(
        self,
        query: str,
        hints: Mapping[str, Any] | None = None,
        limit: int = MAX_SEARCH_RESULTS,
    ) -> tuple[MemorySearchResult, ...]:
        """Return ranked results without a similarity cut-off.

        Metadata hints boost exact or token-overlap matches but never exclude a
        document.  Ties are stable by memory ID.
        """

        normalized_query, query_terms = _validated_query(query)
        normalized_hints = _validated_hints(hints)
        result_limit = _validated_limit(limit)

        ranked: list[
            tuple[int, tuple[int, ...], str, _IndexedMemory, tuple[str, ...]]
        ] = []
        for entry in self._entries:
            field_scores = tuple(
                _field_score(
                    field,
                    normalized_query=normalized_query,
                    query_terms=query_terms,
                )
                for field in entry.fields
            )
            hint_score = _hint_score(entry.metadata, normalized_hints)
            total = sum(field_scores) + hint_score
            matched_fields = tuple(
                field.name
                for field, score in zip(entry.fields, field_scores)
                if score > 0
            )
            ranked.append(
                (
                    total,
                    field_scores,
                    entry.memory_id,
                    entry,
                    matched_fields,
                )
            )

        ranked.sort(key=lambda item: (-item[0], item[2]))
        return tuple(
            MemorySearchResult(
                memory_id=entry.memory_id,
                score=total,
                matched_fields=matched_fields,
                snippet=_best_snippet(
                    entry,
                    field_scores,
                    normalized_query=normalized_query,
                    query_terms=query_terms,
                ),
                metadata=_thaw_json_value(entry.metadata),
            )
            for total, field_scores, _, entry, matched_fields
            in ranked[:result_limit]
        )

    def read(
        self,
        memory_ids: Sequence[str],
        *,
        max_documents: int = MAX_READ_DOCUMENTS,
        max_total_chars: int = MAX_TOTAL_READ_CHARS,
    ) -> tuple[MemoryReadResult, ...]:
        """Read exact IDs in request order, subject to hard content bounds."""

        requested = _validated_memory_ids(memory_ids)
        document_budget = _validated_document_budget(max_documents)
        character_budget = _validated_character_budget(max_total_chars)
        if len(requested) > document_budget:
            raise RetrievalBudgetError(
                "memory read exceeds the document budget"
            )

        unknown = [
            memory_id
            for memory_id in requested
            if memory_id not in self._by_id
        ]
        if unknown:
            raise UnknownMemoryError(
                f"unknown retrieval memory ID: {unknown[0]}"
            )

        total_chars = 0
        results: list[MemoryReadResult] = []
        for memory_id in requested:
            entry = self._by_id[memory_id]
            char_count = len(entry.markdown)
            if char_count > MAX_MEMORY_READ_CHARS:
                raise RetrievalBudgetError(
                    f"retrieval memory {memory_id} exceeds the per-memory limit"
                )
            total_chars += char_count
            if total_chars > character_budget:
                raise RetrievalBudgetError(
                    "memory read exceeds the combined character budget"
                )
            results.append(
                MemoryReadResult(
                    memory_id=memory_id,
                    markdown=entry.markdown,
                    char_count=char_count,
                    metadata=_thaw_json_value(entry.metadata),
                )
            )
        return tuple(results)

    def auto_read_eligibility(
        self,
        *,
        max_documents: int = MAX_READ_DOCUMENTS,
        max_total_chars: int = MAX_TOTAL_READ_CHARS,
    ) -> AutoReadEligibility:
        """Describe whether the complete snapshot can be read exhaustively."""

        document_budget = _validated_document_budget(max_documents)
        character_budget = _validated_character_budget(max_total_chars)
        memory_ids = self.memory_ids
        total_chars = sum(len(entry.markdown) for entry in self._entries)
        if len(memory_ids) > document_budget:
            return AutoReadEligibility(
                eligible=False,
                memory_ids=(),
                total_chars=total_chars,
                reason="too_many_documents",
            )
        if any(
            len(entry.markdown) > MAX_MEMORY_READ_CHARS
            for entry in self._entries
        ) or total_chars > character_budget:
            return AutoReadEligibility(
                eligible=False,
                memory_ids=(),
                total_chars=total_chars,
                reason="content_too_large",
            )
        return AutoReadEligibility(
            eligible=True,
            memory_ids=memory_ids,
            total_chars=total_chars,
            reason="eligible",
        )

    def auto_read_ids(
        self,
        *,
        max_documents: int = MAX_READ_DOCUMENTS,
        max_total_chars: int = MAX_TOTAL_READ_CHARS,
    ) -> tuple[str, ...]:
        """Return every ID only when the whole snapshot fits the read budget."""

        return self.auto_read_eligibility(
            max_documents=max_documents,
            max_total_chars=max_total_chars,
        ).memory_ids


def _index_document(document: MemoryDocument) -> _IndexedMemory:
    metadata = MappingProxyType(
        {
            str(key): _freeze_json_value(value)
            for key, value in sorted(document.frontmatter.items())
        }
    )
    signature = _freeze_json_value(
        document.retrieval_signature
        if document.retrieval_signature is not None
        else {}
    )
    evidence = _freeze_json_value(
        document.episode_evidence
        if document.episode_evidence is not None
        else {}
    )
    prose = _generalized_prose(document.body)
    frontmatter_text = _json_text(metadata)
    signature_text = _json_text(signature)
    evidence_text = _json_text(evidence)
    markdown = _retrieval_markdown(
        metadata=metadata,
        prose=prose,
        signature=signature,
        evidence=evidence,
    )
    fields = tuple(
        _search_field(name, text)
        for name, text in (
            ("retrieval_signature", signature_text),
            ("frontmatter", frontmatter_text),
            ("generalized_prose", prose),
            ("episode_evidence", evidence_text),
        )
    )
    return _IndexedMemory(
        memory_id=document.resolution_id,
        markdown=markdown,
        metadata=metadata,
        fields=fields,
    )


def _document_index_chars(document: MemoryDocument) -> int:
    """Conservatively bound source material before building deep index data."""

    return (
        len(document.body)
        + len(_json_text(_freeze_json_value(document.frontmatter)))
        + len(
            _json_text(
                _freeze_json_value(
                    document.retrieval_signature
                    if document.retrieval_signature is not None
                    else {}
                )
            )
        )
        + len(
            _json_text(
                _freeze_json_value(
                    document.episode_evidence
                    if document.episode_evidence is not None
                    else {}
                )
            )
        )
    )


def _retrieval_markdown(
    *,
    metadata: Mapping[str, Any],
    prose: str,
    signature: Any,
    evidence: Any,
) -> str:
    sections = [
        "# Recovery memory",
        "",
        "## Retrieval Metadata",
        "```json",
        json.dumps(
            _thaw_json_value(metadata),
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ),
        "```",
    ]
    if prose:
        sections.extend(("", "## Generalized Memory", prose))
    sections.extend(
        (
            "",
            "## Retrieval Signature",
            "```json",
            json.dumps(
                _thaw_json_value(signature),
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## Episode Evidence",
            "```json",
            json.dumps(
                _thaw_json_value(evidence),
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        )
    )
    return "\n".join(sections)


def _generalized_prose(body: str) -> str:
    if not isinstance(body, str):
        raise RetrievalError("memory body must be text")
    boundaries = [
        match.start()
        for match in _SECTION_PATTERN.finditer(body)
        if match.group(1).strip().casefold()
        in {
            "retrieval signature",
            "episode evidence",
            "dispatched actions",
        }
    ]
    prose = body[: min(boundaries)] if boundaries else body
    return prose.strip()


def _freeze_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {
                str(key): _freeze_json_value(child)
                for key, child in sorted(
                    value.items(),
                    key=lambda item: str(item[0]),
                )
            }
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json_value(child) for child in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise RetrievalError("retrieval data must contain only JSON values")


def _json_text(value: Any) -> str:
    return json.dumps(
        _thaw_json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _thaw_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _thaw_json_value(child)
            for key, child in value.items()
        }
    if isinstance(value, tuple):
        return [_thaw_json_value(child) for child in value]
    return value


def _search_field(name: str, text: str) -> _SearchField:
    normalized = _normalize_text(text)
    counts: dict[str, int] = {}
    for term in _tokens(normalized):
        counts[term] = counts.get(term, 0) + 1
    return _SearchField(
        name=name,
        text=text,
        normalized=normalized,
        term_counts=MappingProxyType(counts),
    )


def _field_score(
    field: _SearchField,
    *,
    normalized_query: str,
    query_terms: tuple[str, ...],
) -> int:
    weight = _FIELD_WEIGHTS[field.name]
    score = sum(
        weight * min(field.term_counts.get(term, 0), _MAX_TERM_FREQUENCY)
        for term in query_terms
    )
    if normalized_query and normalized_query in field.normalized:
        score += weight * _PHRASE_MULTIPLIER
    return score


def _hint_score(
    metadata: Mapping[str, Any],
    hints: tuple[tuple[str, str, tuple[str, ...]], ...],
) -> int:
    score = 0
    normalized_metadata = {
        str(key).casefold(): _normalize_text(_json_text(value))
        for key, value in metadata.items()
    }
    for key, expected, expected_terms in hints:
        actual = normalized_metadata.get(key.casefold(), "")
        if not actual:
            continue
        if expected == actual:
            score += _HINT_EXACT_BOOST
        else:
            actual_terms = frozenset(_tokens(actual))
            score += _HINT_TOKEN_BOOST * sum(
                1 for term in expected_terms if term in actual_terms
            )
    return score


def _best_snippet(
    entry: _IndexedMemory,
    field_scores: tuple[int, ...],
    *,
    normalized_query: str,
    query_terms: tuple[str, ...],
) -> str:
    candidates = sorted(
        zip(entry.fields, field_scores),
        key=lambda item: (
            -item[1],
            _FIELD_ORDER.index(item[0].name),
        ),
    )
    field = candidates[0][0]
    text = " ".join(field.text.split())
    if not text:
        return ""
    normalized = _normalize_text(text)
    offset = normalized.find(normalized_query) if normalized_query else -1
    if offset < 0:
        offsets = [
            normalized.find(term)
            for term in query_terms
            if normalized.find(term) >= 0
        ]
        offset = min(offsets) if offsets else 0
    start = max(0, offset - MAX_SNIPPET_CHARS // 3)
    end = min(len(text), start + MAX_SNIPPET_CHARS)
    if end - start < MAX_SNIPPET_CHARS:
        start = max(0, end - MAX_SNIPPET_CHARS)
    prefix = "…" if start else ""
    suffix = "…" if end < len(text) else ""
    return f"{field.name}: {prefix}{text[start:end]}{suffix}"


def _validated_query(query: str) -> tuple[str, tuple[str, ...]]:
    if not isinstance(query, str):
        raise RetrievalError("retrieval query must be text")
    stripped = query.strip()
    if not stripped:
        raise RetrievalError("retrieval query cannot be empty")
    if len(stripped) > MAX_QUERY_CHARS:
        raise RetrievalBudgetError("retrieval query exceeds the length limit")
    if "\x00" in stripped:
        raise RetrievalError("retrieval query contains invalid text")
    normalized = _normalize_text(stripped)
    return normalized, tuple(dict.fromkeys(_tokens(normalized)))


def _validated_hints(
    hints: Mapping[str, Any] | None,
) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    if hints is None:
        return ()
    if not isinstance(hints, Mapping):
        raise RetrievalError("retrieval hints must be an object")
    if len(hints) > MAX_SEARCH_HINTS:
        raise RetrievalBudgetError("too many retrieval hints")
    normalized: list[tuple[str, str, tuple[str, ...]]] = []
    for key, value in sorted(hints.items(), key=lambda item: str(item[0])):
        if not isinstance(key, str) or not key.strip():
            raise RetrievalError("retrieval hint names must be text")
        if value is None or (
            isinstance(value, str) and not value.strip()
        ):
            continue
        text = _json_text(_freeze_json_value(value))
        if len(text) > MAX_HINT_CHARS:
            raise RetrievalBudgetError("retrieval hint exceeds the length limit")
        normalized_text = _normalize_text(text)
        normalized.append(
            (
                key.strip(),
                normalized_text,
                tuple(dict.fromkeys(_tokens(normalized_text))),
            )
        )
    return tuple(normalized)


def _validated_limit(limit: int) -> int:
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or limit < 1
        or limit > MAX_SEARCH_RESULTS
    ):
        raise RetrievalBudgetError(
            f"retrieval result limit must be between 1 and {MAX_SEARCH_RESULTS}"
        )
    return limit


def _validated_memory_ids(memory_ids: Sequence[str]) -> tuple[str, ...]:
    if isinstance(memory_ids, (str, bytes)) or not isinstance(
        memory_ids,
        Sequence,
    ):
        raise RetrievalError("memory IDs must be an array")
    requested: list[str] = []
    seen: set[str] = set()
    for memory_id in memory_ids:
        if not isinstance(memory_id, str) or not memory_id.strip():
            raise RetrievalError("memory IDs must be non-empty text")
        canonical = memory_id.strip()
        if canonical in seen:
            raise RetrievalError("memory IDs must not be duplicated")
        seen.add(canonical)
        requested.append(canonical)
    return tuple(requested)


def _validated_document_budget(value: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > MAX_READ_DOCUMENTS
    ):
        raise RetrievalBudgetError(
            f"read document budget must be between 1 and {MAX_READ_DOCUMENTS}"
        )
    return value


def _validated_index_document_budget(value: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > MAX_INDEX_DOCUMENTS
    ):
        raise RetrievalBudgetError(
            "memory index document budget exceeds the hard limit"
        )
    return value


def _validated_index_character_budget(value: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > MAX_INDEX_CHARS
    ):
        raise RetrievalBudgetError(
            "memory index character budget exceeds the hard limit"
        )
    return value


def _validated_character_budget(value: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > MAX_TOTAL_READ_CHARS
    ):
        raise RetrievalBudgetError(
            "read character budget exceeds the hard limit"
        )
    return value


def _normalize_text(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").replace("-", " ").split())


def _tokens(value: str) -> tuple[str, ...]:
    return tuple(_TOKEN_PATTERN.findall(value))
