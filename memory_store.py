"""Markdown-first, Git-backed resolution memory storage.

The model prepares a :class:`MemoryDraft`; this module never calls a model.
The exact dispatched actions always come from the normalized Supabase source,
not from model output. Markdown in Git is authoritative and indexes are rebuilt
by scanning it.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import subprocess
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal, Mapping, Sequence
from urllib.parse import urlsplit
from uuid import UUID


MemoryKind = Literal["positive", "negative"]

_SCHEMA_VERSION = 1
_SOURCE_TABLE = "public.flow_failure_resolutions"
_MEMORIES_DIRECTORY = "memories"
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_BRANCH_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
_FRONTMATTER_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_ACTIONS_PATTERN = re.compile(
    r"(?:^|\n)## Dispatched Actions\n```json\n"
    r"(?P<actions>.*?)\n```(?:\n|$)",
    re.DOTALL,
)

# These are the raw fields that can affect a generated memory. Volatile or
# identifying fields such as operator_email are deliberately excluded.
_SOURCE_FIELDS = (
    "resolution_id",
    "failure_id",
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
    "failure_status",
    "failure_reason",
    "failed_command",
    "failed_action",
    "robot_message",
    "auto_failure_reason",
    "resolution",
    "actions",
    "action_runs",
    "action_specs",
    "robot_version",
    "robot_status",
    "navigation",
    "arm_state",
    "mapping_pose",
    "status_reported_at",
    "flow_snapshot",
    "run_code",
    "outcome",
    "applied",
    "captured_at",
    "resolved_at",
    "created_at",
)
_FRONTMATTER_FIELDS = (
    "schema_version",
    "source",
    "source_hash",
    "resolution_id",
    "failure_id",
    "memory_kind",
    "actionable",
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
    "area_name",
    "item_name",
    "failure_status",
    "failed_command",
    "outcome",
    "applied",
    "action_count",
    "model",
    "response_id",
    "captured_at",
    "resolved_at",
    "created_at",
)

_LOCKS_GUARD = threading.Lock()
_REPOSITORY_LOCKS: dict[Path, threading.RLock] = {}


class GitMemoryError(RuntimeError):
    """Base class for errors that are safe to expose without secret leakage."""


class GitMemoryConfigurationError(GitMemoryError):
    pass


class ResolutionRowError(GitMemoryError):
    pass


class MemoryDraftError(GitMemoryError):
    pass


class MemoryFormatError(GitMemoryError):
    pass


class GitOperationError(GitMemoryError):
    """A redacted Git error: command, URL, stdout, and stderr are omitted."""

    def __init__(self, operation: str, return_code: int | None = None) -> None:
        suffix = (
            f" with exit code {return_code}"
            if return_code is not None
            else ""
        )
        super().__init__(f"Git {operation} failed{suffix}")
        self.operation = operation
        self.return_code = return_code


@dataclass(frozen=True)
class GitMemoryConfig:
    repo_url: str = field(repr=False)
    repo_root: Path
    branch: str = "main"
    author_name: str = "Failure Resolver Agent"
    author_email: str = "failure-resolver@bellboy.co"
    git_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        repo_url = self.repo_url.strip()
        branch = self.branch.strip()
        if not repo_url:
            raise GitMemoryConfigurationError("Repository URL is required")
        if (
            not _BRANCH_PATTERN.fullmatch(branch)
            or ".." in branch
            or "@{" in branch
            or branch.endswith(("/", "."))
        ):
            raise GitMemoryConfigurationError("Branch name is invalid")
        if self.git_timeout_seconds <= 0:
            raise GitMemoryConfigurationError(
                "Git timeout must be greater than zero"
            )
        _validate_identity(self.author_name, "author name")
        _validate_identity(self.author_email, "author email")

        root = Path(self.repo_root).expanduser().resolve(strict=False)
        if root == Path(root.anchor):
            raise GitMemoryConfigurationError(
                "Repository root cannot be a filesystem root"
            )
        object.__setattr__(self, "repo_url", repo_url)
        object.__setattr__(self, "repo_root", root)
        object.__setattr__(self, "branch", branch)


@dataclass(frozen=True)
class ResolutionSource:
    """Canonical input to the memory-building model.

    The canonical JSON is retained privately so callers receive fresh decoded
    values and cannot mutate data behind an already calculated source hash.
    """

    resolution_id: str
    failure_id: str | None
    source_hash: str
    _canonical_payload_json: str = field(repr=False)

    @property
    def payload(self) -> Mapping[str, Any]:
        return json.loads(self._canonical_payload_json)

    @property
    def dispatched_actions(self) -> tuple[Mapping[str, Any], ...]:
        actions = self.payload.get("action_runs", [])
        return tuple(actions)


@dataclass(frozen=True)
class MemoryDraft:
    """Fully prepared model output plus code-owned source evidence."""

    source: ResolutionSource
    memory_kind: MemoryKind
    actionable: bool
    failure_summary: str
    recovery_summary: str
    lessons: tuple[str, ...] = ()
    model: str = "gpt-5.6-luna"
    response_id: str | None = None

    def __post_init__(self) -> None:
        if self.memory_kind not in ("positive", "negative"):
            raise MemoryDraftError("memory_kind must be positive or negative")
        if not isinstance(self.actionable, bool):
            raise MemoryDraftError("actionable must be a boolean")
        failure_summary = _required_draft_text(
            self.failure_summary,
            "failure_summary",
        )
        recovery_summary = _required_draft_text(
            self.recovery_summary,
            "recovery_summary",
        )
        model = _required_draft_text(self.model, "model")
        response_id = _draft_text_or_none(self.response_id, "response_id")
        lessons = tuple(
            _required_draft_text(lesson, "lesson")
            for lesson in self.lessons
        )

        if self.actionable and self.memory_kind != "positive":
            raise MemoryDraftError(
                "Only positive memories can be actionable"
            )
        if self.actionable and not self.source.dispatched_actions:
            raise MemoryDraftError(
                "An actionable memory needs dispatched actions"
            )
        source_outcome = self.source.payload.get("outcome")
        source_applied = self.source.payload.get("applied")
        if self.actionable and (
            source_outcome != "resolved" or source_applied is not True
        ):
            raise MemoryDraftError(
                "Actionable memory requires an applied, resolved source"
            )

        object.__setattr__(self, "failure_summary", failure_summary)
        object.__setattr__(self, "recovery_summary", recovery_summary)
        object.__setattr__(self, "lessons", lessons)
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "response_id", response_id)


@dataclass(frozen=True)
class MemoryWriteResult:
    resolution_id: str
    relative_path: str
    changed: bool
    commit_sha: str | None


@dataclass(frozen=True)
class MemoryDocument:
    path: Path
    frontmatter: Mapping[str, Any]
    dispatched_actions: tuple[Mapping[str, Any], ...]
    body: str

    @property
    def resolution_id(self) -> str:
        return str(self.frontmatter["resolution_id"])

    @property
    def source_hash(self) -> str:
        return str(self.frontmatter["source_hash"])

    @property
    def is_execution_candidate(self) -> bool:
        return (
            self.frontmatter.get("memory_kind") == "positive"
            and self.frontmatter.get("actionable") is True
            and self.frontmatter.get("outcome") == "resolved"
            and self.frontmatter.get("applied") is True
            and bool(self.dispatched_actions)
        )


def resolution_source_from_row(row: Mapping[str, Any]) -> ResolutionSource:
    """Normalize a Supabase resolution row and calculate its source hash."""
    if not isinstance(row, Mapping):
        raise ResolutionRowError("Resolution row must be a mapping")
    resolution_id = _canonical_uuid(
        row.get("resolution_id"),
        "resolution_id",
    )
    raw_failure_id = row.get("failure_id")
    failure_id = (
        _canonical_uuid(raw_failure_id, "failure_id")
        if raw_failure_id is not None
        else None
    )
    applied = row.get("applied", False)
    if not isinstance(applied, bool):
        raise ResolutionRowError("applied must be a boolean")

    payload = {field: row.get(field) for field in _SOURCE_FIELDS}
    payload["resolution_id"] = resolution_id
    payload["failure_id"] = failure_id
    payload["outcome"] = _optional_row_text(row, "outcome") or "recorded"
    payload["applied"] = applied
    navigation = row.get("navigation")
    current_map = (
        navigation.get("current_map")
        if isinstance(navigation, Mapping)
        else None
    )
    if isinstance(current_map, Mapping):
        if payload.get("map_id") is None:
            payload["map_id"] = current_map.get("id")
        if payload.get("map_name") is None:
            payload["map_name"] = current_map.get("map_name")
        if payload.get("map_observed_at") is None:
            payload["map_observed_at"] = row.get("status_reported_at")
    try:
        canonical_payload = json.loads(
            json.dumps(
                payload,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    except (TypeError, ValueError) as error:
        raise ResolutionRowError(
            "Resolution source must contain JSON values"
        ) from error

    action_runs = canonical_payload.get("action_runs")
    if action_runs is None:
        action_runs = []
        canonical_payload["action_runs"] = action_runs
    if not isinstance(action_runs, list) or not all(
        isinstance(action, dict) for action in action_runs
    ):
        raise ResolutionRowError(
            "action_runs must be an array of objects"
        )
    source_json = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    source_hash = hashlib.sha256(source_json.encode("utf-8")).hexdigest()
    return ResolutionSource(
        resolution_id=resolution_id,
        failure_id=failure_id,
        source_hash=source_hash,
        _canonical_payload_json=source_json,
    )


class GitMemoryStore:
    """Persist prepared memories as deterministic Markdown commits."""

    def __init__(self, config: GitMemoryConfig) -> None:
        self.config = config
        self._thread_lock = _repository_lock(config.repo_root)
        self._lock_path = (
            config.repo_root.parent / f".{config.repo_root.name}.memory.lock"
        )

    def write_memory(self, draft: MemoryDraft) -> MemoryWriteResult:
        """Pull, atomically write, commit, and push one prepared draft."""
        if not isinstance(draft, MemoryDraft):
            raise MemoryDraftError("write_memory requires a MemoryDraft")
        content = _render_memory(draft)
        relative_path = (
            Path(_MEMORIES_DIRECTORY)
            / f"{draft.source.resolution_id}.md"
        )

        with self._thread_lock, self._process_lock():
            self._ensure_checkout()
            self._pull_ff_only()
            target = self._safe_memory_path(draft.source.resolution_id)
            if not target.exists() or target.read_text(encoding="utf-8") != content:
                _atomic_write_text(target, content)

            self._git(
                ["add", "--", relative_path.as_posix()],
                operation="stage memory",
            )
            changed = self._has_staged_change(relative_path)
            commit_sha: str | None = None
            if changed:
                self._commit_memory(relative_path, draft)
                commit_sha = self._git(
                    ["rev-parse", "HEAD"],
                    operation="read commit",
                )

            # Push even on a no-op to recover a prior local commit whose push
            # was interrupted.
            self._push()
            return MemoryWriteResult(
                resolution_id=draft.source.resolution_id,
                relative_path=relative_path.as_posix(),
                changed=changed,
                commit_sha=commit_sha,
            )

    async def awrite_memory(self, draft: MemoryDraft) -> MemoryWriteResult:
        return await asyncio.to_thread(self.write_memory, draft)

    def rebuild_index(
        self,
        *,
        refresh: bool = True,
    ) -> dict[str, MemoryDocument]:
        with self._thread_lock, self._process_lock():
            self._ensure_checkout()
            if refresh:
                self._pull_ff_only()
            return self._scan_memories()

    async def arebuild_index(
        self,
        *,
        refresh: bool = True,
    ) -> dict[str, MemoryDocument]:
        return await asyncio.to_thread(
            self.rebuild_index,
            refresh=refresh,
        )

    def has_source_hash(
        self,
        resolution_id: str,
        source_hash: str,
        *,
        refresh: bool = True,
    ) -> bool:
        canonical_id = _canonical_uuid(resolution_id, "resolution_id")
        if not isinstance(source_hash, str) or not _HASH_PATTERN.fullmatch(
            source_hash
        ):
            raise ResolutionRowError("source_hash must be a SHA-256 digest")
        document = self.rebuild_index(refresh=refresh).get(canonical_id)
        return document is not None and document.source_hash == source_hash

    async def ahas_source_hash(
        self,
        resolution_id: str,
        source_hash: str,
        *,
        refresh: bool = True,
    ) -> bool:
        return await asyncio.to_thread(
            self.has_source_hash,
            resolution_id,
            source_hash,
            refresh=refresh,
        )

    def execution_candidates(
        self,
        *,
        refresh: bool = True,
    ) -> tuple[MemoryDocument, ...]:
        index = self.rebuild_index(refresh=refresh)
        return tuple(
            document
            for _, document in sorted(index.items())
            if document.is_execution_candidate
        )

    def _ensure_checkout(self) -> None:
        root = self.config.repo_root
        if not root.exists():
            root.parent.mkdir(parents=True, exist_ok=True)
            self._run(
                [
                    "git",
                    "clone",
                    "--branch",
                    self.config.branch,
                    "--single-branch",
                    "--",
                    self.config.repo_url,
                    str(root),
                ],
                operation="clone",
            )
        elif not root.is_dir() or root.is_symlink():
            raise GitMemoryConfigurationError(
                "Repository root must be a non-symlink directory"
            )

        if self._git(
            ["rev-parse", "--is-inside-work-tree"],
            operation="verify checkout",
        ) != "true":
            raise GitMemoryConfigurationError(
                "Repository root is not a Git worktree"
            )
        if self._git(
            ["branch", "--show-current"],
            operation="read branch",
        ) != self.config.branch:
            raise GitMemoryConfigurationError(
                "Checkout is on a different branch"
            )
        actual_origin = self._git(
            ["remote", "get-url", "origin"],
            operation="read origin",
        )
        if _canonical_repo_locator(actual_origin) != _canonical_repo_locator(
            self.config.repo_url
        ):
            raise GitMemoryConfigurationError(
                "Checkout origin does not match configured repository"
            )

    def _pull_ff_only(self) -> None:
        self._git(
            ["pull", "--ff-only", "origin", self.config.branch],
            operation="pull",
        )

    def _push(self) -> None:
        self._git(
            [
                "push",
                "origin",
                f"HEAD:refs/heads/{self.config.branch}",
            ],
            operation="push",
        )

    def _commit_memory(
        self,
        relative_path: Path,
        draft: MemoryDraft,
    ) -> None:
        source = draft.source
        message = (
            f"Record resolution memory {source.resolution_id}\n\n"
            "Agent: failure-resolver\n"
            f"Source-Table: {_SOURCE_TABLE}\n"
            f"Source-Hash: {source.source_hash}\n"
            f"Memory-Kind: {draft.memory_kind}\n"
            f"Resolution-ID: {source.resolution_id}\n"
            f"Failure-ID: {source.failure_id or 'none'}\n"
        )
        self._git(
            [
                "-c",
                f"user.name={self.config.author_name}",
                "-c",
                f"user.email={self.config.author_email}",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--only",
                "--file=-",
                "--",
                relative_path.as_posix(),
            ],
            operation="commit memory",
            input_text=message,
        )

    def _has_staged_change(self, relative_path: Path) -> bool:
        completed = self._run(
            [
                "git",
                "-C",
                str(self.config.repo_root),
                "diff",
                "--cached",
                "--quiet",
                "--",
                relative_path.as_posix(),
            ],
            operation="inspect staged memory",
            allowed_return_codes=(0, 1),
        )
        return completed.returncode == 1

    def _scan_memories(self) -> dict[str, MemoryDocument]:
        memory_dir = self._safe_memories_directory(create=False)
        if not memory_dir.exists():
            return {}
        index: dict[str, MemoryDocument] = {}
        for path in sorted(memory_dir.glob("*.md")):
            if path.is_symlink() or not path.is_file():
                raise MemoryFormatError(
                    "Memory entries must be regular Markdown files"
                )
            document = parse_memory_document(path)
            if path.stem != document.resolution_id:
                raise MemoryFormatError(
                    "Memory filename does not match resolution_id"
                )
            if document.resolution_id in index:
                raise MemoryFormatError("Duplicate resolution_id in memories")
            index[document.resolution_id] = document
        return index

    def _safe_memories_directory(self, *, create: bool) -> Path:
        memory_dir = self.config.repo_root / _MEMORIES_DIRECTORY
        if memory_dir.is_symlink():
            raise GitMemoryConfigurationError(
                "Memories directory cannot be a symlink"
            )
        if memory_dir.exists() and not memory_dir.is_dir():
            raise GitMemoryConfigurationError(
                "Memories path must be a directory"
            )
        if create:
            memory_dir.mkdir(mode=0o755, parents=False, exist_ok=True)
        if memory_dir.exists() and memory_dir.resolve() != memory_dir:
            raise GitMemoryConfigurationError(
                "Memories directory escapes the repository"
            )
        return memory_dir

    def _safe_memory_path(self, resolution_id: str) -> Path:
        memory_dir = self._safe_memories_directory(create=True)
        target = memory_dir / f"{resolution_id}.md"
        if target.is_symlink():
            raise GitMemoryConfigurationError(
                "Memory path cannot be a symlink"
            )
        if target.parent.resolve() != memory_dir.resolve():
            raise GitMemoryConfigurationError(
                "Memory path escapes the memories directory"
            )
        return target

    def _git(
        self,
        arguments: Sequence[str],
        *,
        operation: str,
        input_text: str | None = None,
    ) -> str:
        completed = self._run(
            ["git", "-C", str(self.config.repo_root), *arguments],
            operation=operation,
            input_text=input_text,
        )
        return completed.stdout.strip()

    def _run(
        self,
        arguments: Sequence[str],
        *,
        operation: str,
        input_text: str | None = None,
        allowed_return_codes: Sequence[int] = (0,),
    ) -> subprocess.CompletedProcess[str]:
        try:
            completed = subprocess.run(
                list(arguments),
                input=input_text,
                capture_output=True,
                check=False,
                shell=False,
                text=True,
                timeout=self.config.git_timeout_seconds,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise GitOperationError(operation) from error
        if completed.returncode not in allowed_return_codes:
            raise GitOperationError(operation, completed.returncode)
        return completed

    @contextmanager
    def _process_lock(self) -> Iterator[None]:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = os.fdopen(
            os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o600),
            "a+",
            encoding="utf-8",
        )
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            finally:
                lock_file.close()


def parse_memory_document(path: Path) -> MemoryDocument:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as error:
        raise MemoryFormatError("Could not read memory Markdown") from error
    if not content.startswith("---\n"):
        raise MemoryFormatError("Memory is missing YAML frontmatter")
    closing_offset = content.find("\n---\n", 4)
    if closing_offset < 0:
        raise MemoryFormatError("Memory frontmatter is not terminated")
    frontmatter = _parse_frontmatter(content[4:closing_offset])
    body = content[closing_offset + 5 :]

    resolution_id = _canonical_uuid(
        frontmatter.get("resolution_id"),
        "resolution_id",
        error_type=MemoryFormatError,
    )
    frontmatter["resolution_id"] = resolution_id
    source_hash = frontmatter.get("source_hash")
    if not isinstance(source_hash, str) or not _HASH_PATTERN.fullmatch(
        source_hash
    ):
        raise MemoryFormatError("Memory source_hash is invalid")

    actions_match = _ACTIONS_PATTERN.search(body)
    if actions_match is None:
        raise MemoryFormatError(
            "Memory is missing dispatched-actions JSON"
        )
    try:
        actions = json.loads(actions_match.group("actions"))
    except (TypeError, ValueError) as error:
        raise MemoryFormatError(
            "Dispatched-actions JSON is invalid"
        ) from error
    if not isinstance(actions, list) or not all(
        isinstance(action, dict) for action in actions
    ):
        raise MemoryFormatError(
            "Dispatched actions must be an array of objects"
        )
    if frontmatter.get("action_count") != len(actions):
        raise MemoryFormatError(
            "Memory action_count does not match dispatched actions"
        )

    return MemoryDocument(
        path=path,
        frontmatter=frontmatter,
        dispatched_actions=tuple(actions),
        body=body,
    )


def _render_memory(draft: MemoryDraft) -> str:
    source = draft.source
    payload = source.payload
    dispatched_actions = source.dispatched_actions
    frontmatter = {
        "schema_version": _SCHEMA_VERSION,
        "source": _SOURCE_TABLE,
        "source_hash": source.source_hash,
        "resolution_id": source.resolution_id,
        "failure_id": source.failure_id,
        "memory_kind": draft.memory_kind,
        "actionable": draft.actionable,
        "sysid": payload.get("sysid"),
        "site_id": payload.get("site_id"),
        "site": payload.get("site"),
        "floor": payload.get("floor"),
        "room_number": payload.get("room_number"),
        "map_id": payload.get("map_id"),
        "map_name": payload.get("map_name"),
        "map_observed_at": payload.get("map_observed_at"),
        "flow_id": payload.get("flow_id"),
        "flow_name": payload.get("flow_name"),
        "area_name": payload.get("area_name"),
        "item_name": payload.get("item_name"),
        "failure_status": payload.get("failure_status"),
        "failed_command": payload.get("failed_command"),
        "outcome": payload.get("outcome"),
        "applied": payload.get("applied"),
        "action_count": len(dispatched_actions),
        "model": draft.model,
        "response_id": draft.response_id,
        "captured_at": payload.get("captured_at"),
        "resolved_at": payload.get("resolved_at"),
        "created_at": payload.get("created_at"),
    }
    frontmatter_lines = ["---"] + [
        f"{key}: {_yaml_scalar(frontmatter[key])}"
        for key in _FRONTMATTER_FIELDS
    ] + ["---"]
    actions_json = json.dumps(
        list(dispatched_actions),
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
        sort_keys=True,
    )
    # Model-generated prose is quoted line-by-line. It therefore cannot inject
    # a second executable-looking JSON section into the Markdown structure.
    lessons = (
        "\n".join(
            _blockquote(f"{index}. {lesson}")
            for index, lesson in enumerate(draft.lessons, start=1)
        )
        if draft.lessons
        else "_None recorded._"
    )
    context = [
        f"- Robot: {_body_scalar(payload.get('sysid'))}",
        f"- Site ID: {_body_scalar(payload.get('site_id'))}",
        f"- Site: {_body_scalar(payload.get('site'))}",
        f"- Floor: {_body_scalar(payload.get('floor'))}",
        f"- Room: {_body_scalar(payload.get('room_number'))}",
        f"- Map ID: {_body_scalar(payload.get('map_id'))}",
        f"- Map: {_body_scalar(payload.get('map_name'))}",
        f"- Map observed at: {_body_scalar(payload.get('map_observed_at'))}",
        f"- Flow: {_body_scalar(payload.get('flow_name'))}",
        f"- Area: {_body_scalar(payload.get('area_name'))}",
        f"- Item: {_body_scalar(payload.get('item_name'))}",
        f"- Failed command: {_body_scalar(payload.get('failed_command'))}",
        f"- Outcome: {_body_scalar(payload.get('outcome'))}",
        f"- Applied: {'yes' if payload.get('applied') else 'no'}",
        f"- Actionable: {'yes' if draft.actionable else 'no'}",
    ]
    return "\n".join(
        [
            *frontmatter_lines,
            "",
            f"# Resolution Memory {source.resolution_id}",
            "",
            "## Failure Pattern",
            _blockquote(draft.failure_summary),
            "",
            "## Recovery Knowledge",
            _blockquote(draft.recovery_summary),
            "",
            "## Lessons",
            lessons,
            "",
            "## Context",
            *context,
            "",
            "## Dispatched Actions",
            "```json",
            actions_json,
            "```",
            "",
        ]
    )


def _parse_frontmatter(text: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for line in text.splitlines():
        key, separator, raw_value = line.partition(":")
        key = key.strip()
        if not separator or not _FRONTMATTER_KEY_PATTERN.fullmatch(key):
            raise MemoryFormatError(
                "Memory frontmatter contains an invalid field"
            )
        if key in parsed:
            raise MemoryFormatError(
                "Memory frontmatter contains duplicate fields"
            )
        value_text = raw_value.strip()
        try:
            parsed[key] = json.loads(value_text)
        except (TypeError, ValueError):
            parsed[key] = value_text

    if set(_FRONTMATTER_FIELDS) - parsed.keys():
        raise MemoryFormatError(
            "Memory frontmatter is missing required fields"
        )
    if parsed.get("schema_version") != _SCHEMA_VERSION:
        raise MemoryFormatError("Memory schema version is unsupported")
    if parsed.get("source") != _SOURCE_TABLE:
        raise MemoryFormatError("Memory source is unsupported")
    if parsed.get("memory_kind") not in ("positive", "negative"):
        raise MemoryFormatError("Memory kind is invalid")
    if not isinstance(parsed.get("actionable"), bool):
        raise MemoryFormatError("Memory actionable flag is invalid")
    action_count = parsed.get("action_count")
    if (
        not isinstance(action_count, int)
        or isinstance(action_count, bool)
        or action_count < 0
    ):
        raise MemoryFormatError("Memory action_count is invalid")
    return parsed


def _atomic_write_text(path: Path, content: str) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.stem}.",
        suffix=".tmp",
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temporary_path, 0o644)
        os.replace(temporary_path, path)
        try:
            directory_descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError:
            pass
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass


def _repository_lock(repo_root: Path) -> threading.RLock:
    with _LOCKS_GUARD:
        return _REPOSITORY_LOCKS.setdefault(repo_root, threading.RLock())


def _canonical_uuid(
    value: Any,
    field_name: str,
    *,
    error_type: type[GitMemoryError] = ResolutionRowError,
) -> str:
    if not isinstance(value, str) or not value.strip():
        raise error_type(f"{field_name} must be a UUID")
    try:
        return str(UUID(value.strip()))
    except ValueError as error:
        raise error_type(f"{field_name} must be a UUID") from error


def _optional_row_text(row: Mapping[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ResolutionRowError(f"{key} must be text or null")
    return value.strip() or None


def _required_draft_text(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or "\x00" in value
    ):
        raise MemoryDraftError(f"{field_name} must be non-empty text")
    return value.strip()


def _draft_text_or_none(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or "\x00" in value:
        raise MemoryDraftError(f"{field_name} must be text or null")
    return value.strip() or None


def _yaml_scalar(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def _blockquote(value: str) -> str:
    return "\n".join(
        f"> {line}" if line else ">"
        for line in value.splitlines()
    )


def _body_scalar(value: Any) -> str:
    if value is None:
        return "Not recorded"
    return " ".join(str(value).splitlines())


def _canonical_repo_locator(value: str) -> str:
    stripped = value.strip().rstrip("/")
    parsed = urlsplit(stripped)
    if parsed.scheme or stripped.startswith("git@"):
        return stripped
    return str(Path(stripped).expanduser().resolve(strict=False))


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not value
        or len(value) > 200
        or any(character in value for character in ("\r", "\n", "\x00"))
    ):
        raise GitMemoryConfigurationError(f"Git {field_name} is invalid")
