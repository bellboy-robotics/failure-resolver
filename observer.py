"""Read-only Supabase observer for failure events.

This is the default container entrypoint. It deliberately does not import or
initialize the legacy resolver, language models, vector storage, SQS, or robot
control code.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


logger = logging.getLogger("failure_resolver.observer")

ClientFactory = Callable[[str, str], Awaitable[Any]]
_POSTGRES_EVENTS = ("INSERT", "UPDATE")
_SAFE_ROW_COLUMNS = ("failure_id", "sysid", "flow_id", "matcher_status")
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SENSITIVE_DEPENDENCY_LOGGERS = (
    "gotrue",
    "httpcore",
    "httpx",
    "postgrest",
    "realtime",
    "storage3",
    "supabase",
    "supafunc",
    "websockets",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _error_type(error: BaseException | None, fallback: str) -> str:
    """Return a safe error classification without leaking URLs or credentials."""
    return type(error).__name__ if error is not None else fallback


def _status_name(status: Any) -> str:
    value = getattr(status, "value", status)
    return str(value).rsplit(".", 1)[-1].upper()


def _safe_scalar(value: Any, *, max_length: int = 160) -> str | None:
    """Convert a scalar identifier to bounded, log-safe text."""
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return None
    text = str(value)
    text = "".join(character if character.isprintable() else "\ufffd" for character in text)
    return text[:max_length]


@dataclass(frozen=True)
class ObserverSettings:
    supabase_url: str
    supabase_service_role_key: str = field(repr=False)
    failure_events_table: str = "failure_events"
    schema: str = "public"
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    subscription_timeout_seconds: float = 20.0
    connection_check_seconds: float = 1.0
    disconnect_grace_seconds: float = 10.0
    shutdown_timeout_seconds: float = 5.0

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> "ObserverSettings":
        environment = environment or os.environ
        mode = environment.get("RESOLVER_MODE", "observe").strip().lower()
        if mode != "observe":
            raise ValueError("The observer entrypoint requires RESOLVER_MODE=observe")

        url = environment.get("SUPABASE_URL", "").strip()
        key = environment.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        table = environment.get("FAILURE_EVENTS_TABLE", "failure_events").strip()
        schema = environment.get("SUPABASE_SCHEMA", "public").strip()

        if not url.startswith(("https://", "http://")):
            raise ValueError("SUPABASE_URL must be an http(s) URL")
        if not key:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY is required")
        for variable, value in (
            ("FAILURE_EVENTS_TABLE", table),
            ("SUPABASE_SCHEMA", schema),
        ):
            if not _VALID_IDENTIFIER.fullmatch(value):
                raise ValueError(f"{variable} must be a PostgreSQL identifier")

        return cls(
            supabase_url=url.rstrip("/"),
            supabase_service_role_key=key,
            failure_events_table=table,
            schema=schema,
            reconnect_initial_seconds=_positive_float(
                environment, "OBSERVER_RECONNECT_INITIAL_SECONDS", 1.0
            ),
            reconnect_max_seconds=_positive_float(
                environment, "OBSERVER_RECONNECT_MAX_SECONDS", 30.0
            ),
            subscription_timeout_seconds=_positive_float(
                environment, "OBSERVER_SUBSCRIPTION_TIMEOUT_SECONDS", 20.0
            ),
            connection_check_seconds=_positive_float(
                environment, "OBSERVER_CONNECTION_CHECK_SECONDS", 1.0
            ),
            disconnect_grace_seconds=_positive_float(
                environment, "OBSERVER_DISCONNECT_GRACE_SECONDS", 10.0
            ),
            shutdown_timeout_seconds=_positive_float(
                environment, "OBSERVER_SHUTDOWN_TIMEOUT_SECONDS", 5.0
            ),
        )


def _positive_float(environment: Mapping[str, str], name: str, default: float) -> float:
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


@dataclass
class ObserverState:
    running: bool = False
    connected: bool = False
    events_observed: int = 0
    rows_fetched: int = 0
    fetch_errors: int = 0
    events_dropped: int = 0
    reconnect_attempts: int = 0
    started_at: str | None = None
    last_connected_at: str | None = None
    last_event_at: str | None = None
    last_error_type: str | None = None

    def snapshot(self) -> dict[str, Any]:
        if self.connected:
            status = "connected"
        elif self.running:
            status = "connecting"
        else:
            status = "stopped"
        return {
            "mode": "observe",
            "status": status,
            "connected": self.connected,
            "events_observed": self.events_observed,
            "rows_fetched": self.rows_fetched,
            "fetch_errors": self.fetch_errors,
            "events_dropped": self.events_dropped,
            "reconnect_attempts": self.reconnect_attempts,
            "started_at": self.started_at,
            "last_connected_at": self.last_connected_at,
            "last_event_at": self.last_event_at,
            "last_error_type": self.last_error_type,
        }


@dataclass(frozen=True)
class SafeFailureEvent:
    event: str | None
    failure_id: str | None
    commit_timestamp: str | None

    @classmethod
    def from_realtime_payload(cls, payload: Any) -> "SafeFailureEvent":
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        record = data.get("record", {}) if isinstance(data, dict) else {}
        if not isinstance(record, dict):
            record = {}
        return cls(
            event=_safe_scalar(data.get("type")),
            failure_id=_safe_scalar(record.get("failure_id")),
            commit_timestamp=_safe_scalar(data.get("commit_timestamp")),
        )


@dataclass(frozen=True)
class ChangeSignal:
    event: str | None
    failure_id: str
    commit_timestamp: str | None


async def create_supabase_client(url: str, key: str) -> Any:
    """Import supabase-py lazily so tests can inject a small fake client."""
    from supabase import acreate_client

    return await acreate_client(url, key)


class SupabaseFailureObserver:
    def __init__(
        self,
        settings: ObserverSettings,
        client_factory: ClientFactory = create_supabase_client,
    ) -> None:
        self.settings = settings
        self.state = ObserverState()
        self._client_factory = client_factory
        self._client: Any | None = None
        self._channel: Any | None = None
        self._stop_event = asyncio.Event()
        self._subscription_event = asyncio.Event()
        self._reconnect_event = asyncio.Event()
        self._subscription_error: BaseException | None = None
        self._fetch_queue: asyncio.Queue[ChangeSignal] = asyncio.Queue(maxsize=256)

    async def run(self) -> None:
        self.state.running = True
        self.state.started_at = _utc_now()
        retry_delay = self.settings.reconnect_initial_seconds
        try:
            while not self._stop_event.is_set():
                fetch_worker: asyncio.Task[Any] | None = None
                try:
                    await self._connect()
                    fetch_worker = asyncio.create_task(
                        self._fetch_worker(),
                        name="failure-events-safe-row-fetcher",
                    )
                    retry_delay = self.settings.reconnect_initial_seconds
                    await self._wait_until_stopped_or_disconnected()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._mark_disconnected(_error_type(error, "ConnectionError"))
                finally:
                    await self._stop_fetch_worker(fetch_worker)
                    await self._disconnect()

                if self._stop_event.is_set():
                    break

                self.state.reconnect_attempts += 1
                logger.warning(
                    "Supabase observer reconnect scheduled error_type=%s delay_seconds=%s",
                    self.state.last_error_type,
                    retry_delay,
                )
                await self._wait_for_stop(retry_delay)
                retry_delay = min(
                    retry_delay * 2,
                    self.settings.reconnect_max_seconds,
                )
        finally:
            self.state.running = False
            self.state.connected = False
            await self._disconnect()

    async def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_event.set()

    async def _connect(self) -> None:
        self._subscription_event = asyncio.Event()
        self._reconnect_event = asyncio.Event()
        self._subscription_error = None
        self.state.connected = False

        self._client = await self._client_factory(
            self.settings.supabase_url,
            self.settings.supabase_service_role_key,
        )
        self._channel = self._client.channel(
            f"failure-resolver:{self.settings.schema}:{self.settings.failure_events_table}"
        )
        for event in _POSTGRES_EVENTS:
            self._channel.on_postgres_changes(
                event,
                schema=self.settings.schema,
                table=self.settings.failure_events_table,
                callback=self._on_change,
            )

        await self._channel.subscribe(self._on_subscription_status)
        await asyncio.wait_for(
            self._subscription_event.wait(),
            timeout=self.settings.subscription_timeout_seconds,
        )
        if self._subscription_error is not None:
            raise self._subscription_error
        if not self.state.connected:
            raise ConnectionError("Supabase subscription did not become ready")

    def _on_subscription_status(
        self,
        status: Any,
        error: BaseException | None = None,
    ) -> None:
        name = _status_name(status)
        if name == "SUBSCRIBED":
            self.state.connected = True
            self.state.last_connected_at = _utc_now()
            self.state.last_error_type = None
            self._subscription_event.set()
            logger.info(
                "Supabase observer subscribed schema=%s table=%s",
                self.settings.schema,
                self.settings.failure_events_table,
            )
            return

        self._subscription_error = error or ConnectionError(name)
        self._mark_disconnected(_error_type(error, name))
        self._subscription_event.set()
        self._reconnect_event.set()

    def _on_change(self, payload: Any) -> None:
        event = SafeFailureEvent.from_realtime_payload(payload)
        self.state.events_observed += 1
        self.state.last_event_at = _utc_now()
        if event.failure_id is None:
            self.state.events_dropped += 1
            logger.warning(
                "Ignoring failure_event change without failure_id event=%r",
                event.event,
            )
            return

        try:
            self._fetch_queue.put_nowait(
                ChangeSignal(
                    event=event.event,
                    failure_id=event.failure_id,
                    commit_timestamp=event.commit_timestamp,
                )
            )
        except asyncio.QueueFull:
            self.state.events_dropped += 1
            logger.warning(
                "Failure-event fetch queue full event=%r failure_id=%r",
                event.event,
                event.failure_id,
            )

    async def _fetch_worker(self) -> None:
        while True:
            signal = await self._fetch_queue.get()
            try:
                await self._fetch_and_log_safe_row(signal)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.state.fetch_errors += 1
                logger.warning(
                    (
                        "Failure-event row fetch failed event=%r failure_id=%r "
                        "error_type=%s"
                    ),
                    signal.event,
                    signal.failure_id,
                    type(error).__name__,
                )
            finally:
                self._fetch_queue.task_done()

    async def _fetch_and_log_safe_row(self, signal: ChangeSignal) -> None:
        client = self._client
        if client is None:
            raise ConnectionError("Supabase client is unavailable")

        response = await (
            client.table(self.settings.failure_events_table)
            .select(",".join(_SAFE_ROW_COLUMNS))
            .eq("failure_id", signal.failure_id)
            .limit(1)
            .execute()
        )
        response_data = getattr(response, "data", None)
        if not isinstance(response_data, list) or not response_data:
            logger.info(
                "Failure-event row no longer present event=%r failure_id=%r",
                signal.event,
                signal.failure_id,
            )
            return

        row = response_data[0] if isinstance(response_data[0], dict) else {}
        self.state.rows_fetched += 1
        logger.info(
            (
                "Read failure_event change event=%r failure_id=%r sysid=%r "
                "flow_id=%r matcher_status=%r commit_timestamp=%r"
            ),
            signal.event,
            _safe_scalar(row.get("failure_id")),
            _safe_scalar(row.get("sysid")),
            _safe_scalar(row.get("flow_id")),
            _safe_scalar(row.get("matcher_status")),
            signal.commit_timestamp,
        )

    async def _stop_fetch_worker(
        self,
        fetch_worker: asyncio.Task[Any] | None,
    ) -> None:
        if fetch_worker is None:
            return
        try:
            await asyncio.wait_for(
                self._fetch_queue.join(),
                timeout=self.settings.shutdown_timeout_seconds,
            )
        except asyncio.TimeoutError:
            pass
        fetch_worker.cancel()
        await asyncio.gather(fetch_worker, return_exceptions=True)

    async def _wait_until_stopped_or_disconnected(self) -> None:
        unhealthy_since: float | None = None
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            if self._reconnect_event.is_set():
                raise ConnectionError("Supabase subscription reported an error")

            if self._connection_is_healthy():
                unhealthy_since = None
                self.state.connected = True
            else:
                self.state.connected = False
                unhealthy_since = unhealthy_since or loop.time()
                if loop.time() - unhealthy_since >= self.settings.disconnect_grace_seconds:
                    raise ConnectionError("Supabase connection remained unavailable")

            await self._wait_for_stop(self.settings.connection_check_seconds)

    def _connection_is_healthy(self) -> bool:
        if self._client is None or self._channel is None:
            return False
        channel_joined = bool(getattr(self._channel, "is_joined", True))
        realtime = getattr(self._client, "realtime", None)
        socket_connected = bool(getattr(realtime, "is_connected", True))
        return channel_joined and socket_connected

    def _mark_disconnected(self, error_type: str) -> None:
        self.state.connected = False
        self.state.last_error_type = error_type

    async def _wait_for_stop(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def _disconnect(self) -> None:
        client, channel = self._client, self._channel
        self._client = None
        self._channel = None
        self.state.connected = False
        if client is None:
            return
        try:
            if channel is not None:
                await client.remove_channel(channel)
            realtime = getattr(client, "realtime", None)
            if realtime is not None and bool(getattr(realtime, "is_connected", False)):
                await realtime.close()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "Supabase observer cleanup failed error_type=%s",
                type(error).__name__,
            )


def create_app(
    *,
    settings: ObserverSettings | None = None,
    client_factory: ClientFactory = create_supabase_client,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        observer = SupabaseFailureObserver(
            settings or ObserverSettings.from_env(),
            client_factory=client_factory,
        )
        app.state.observer = observer
        observer_task = asyncio.create_task(
            observer.run(),
            name="supabase-failure-events-observer",
        )
        app.state.observer_task = observer_task
        await asyncio.sleep(0)
        try:
            yield
        finally:
            await observer.stop()
            try:
                await asyncio.wait_for(
                    observer_task,
                    timeout=observer.settings.shutdown_timeout_seconds,
                )
            except asyncio.TimeoutError:
                observer_task.cancel()
                await asyncio.gather(observer_task, return_exceptions=True)

    application = FastAPI(
        title="Billie Failure Resolver Observer",
        version="0.1.0",
        lifespan=lifespan,
    )

    @application.get("/health")
    async def health(request: Request) -> JSONResponse:
        observer = getattr(request.app.state, "observer", None)
        observer_task = getattr(request.app.state, "observer_task", None)
        if observer is None:
            return JSONResponse(
                content={
                    "mode": "observe",
                    "status": "starting",
                    "connected": False,
                },
                status_code=503,
            )
        snapshot = observer.state.snapshot()
        task_done = observer_task is not None and observer_task.done()
        live = observer.state.running and not task_done
        return JSONResponse(
            content=snapshot,
            status_code=200 if live else 503,
        )

    @application.get("/readyz")
    async def ready(request: Request) -> JSONResponse:
        observer = getattr(request.app.state, "observer", None)
        snapshot = (
            observer.state.snapshot()
            if observer is not None
            else {
                "mode": "observe",
                "status": "starting",
                "connected": False,
            }
        )
        return JSONResponse(
            content=snapshot,
            status_code=200 if snapshot["connected"] else 503,
        )

    return application


def _configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for logger_name in _SENSITIVE_DEPENDENCY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


app = create_app()


if __name__ == "__main__":
    _configure_logging()
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "INFO").lower(),
    )
