import asyncio
import logging

import httpx
import pytest

from observer import (
    _SENSITIVE_DEPENDENCY_LOGGERS,
    _configure_logging,
    ObserverSettings,
    SafeFailureEvent,
    SupabaseFailureObserver,
    create_app,
)


class FakeRealtime:
    def __init__(self) -> None:
        self.is_connected = True
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        self.is_connected = False


class FakeChannel:
    def __init__(self) -> None:
        self.is_joined = False
        self.bindings = {}
        self.status_callback = None

    def on_postgres_changes(
        self,
        event,
        *,
        schema,
        table,
        callback,
    ):
        self.bindings[event] = {
            "schema": schema,
            "table": table,
            "callback": callback,
        }
        return self

    async def subscribe(self, callback) -> None:
        self.status_callback = callback
        self.is_joined = True
        callback("SUBSCRIBED", None)

    def emit(self, event, payload) -> None:
        self.bindings[event]["callback"](payload)


class FakeClient:
    def __init__(self, rows=None) -> None:
        self.realtime = FakeRealtime()
        self.channel_instance = FakeChannel()
        self.channel_name = None
        self.remove_calls = 0
        self.rows = rows or []
        self.queries = []

    def channel(self, name):
        self.channel_name = name
        return self.channel_instance

    async def remove_channel(self, channel) -> None:
        assert channel is self.channel_instance
        self.remove_calls += 1
        channel.is_joined = False

    def table(self, table):
        query = FakeQuery(self, table)
        self.queries.append(query)
        return query


class FakeQuery:
    def __init__(self, client, table) -> None:
        self.client = client
        self.table = table
        self.columns = None
        self.filter = None
        self.limit_value = None

    def select(self, columns):
        self.columns = columns
        return self

    def eq(self, column, value):
        self.filter = (column, value)
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    async def execute(self):
        column, value = self.filter
        rows = [row for row in self.client.rows if row.get(column) == value]
        return FakeResponse(rows[: self.limit_value])


class FakeResponse:
    def __init__(self, data) -> None:
        self.data = data


def observer_settings(**overrides):
    values = {
        "supabase_url": "https://project.supabase.co",
        "supabase_service_role_key": "service-role-secret",
        "failure_events_table": "failure_events",
        "reconnect_initial_seconds": 0.001,
        "reconnect_max_seconds": 0.002,
        "subscription_timeout_seconds": 0.1,
        "connection_check_seconds": 0.001,
        "disconnect_grace_seconds": 0.01,
        "shutdown_timeout_seconds": 0.1,
    }
    values.update(overrides)
    return ObserverSettings(**values)


async def wait_until(predicate, *, timeout=0.25):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition did not become true")
        await asyncio.sleep(0.001)


def test_settings_use_deployment_variable_names_and_hide_key():
    settings = ObserverSettings.from_env(
        {
            "RESOLVER_MODE": "observe",
            "SUPABASE_URL": "https://project.supabase.co/",
            "SUPABASE_SERVICE_ROLE_KEY": "top-secret",
            "FAILURE_EVENTS_TABLE": "failure_events",
        }
    )

    assert settings.supabase_url == "https://project.supabase.co"
    assert settings.supabase_service_role_key == "top-secret"
    assert settings.failure_events_table == "failure_events"
    assert "top-secret" not in repr(settings)


@pytest.mark.parametrize(
    "environment, error",
    [
        (
            {
                "RESOLVER_MODE": "full",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "key",
            },
            "RESOLVER_MODE=observe",
        ),
        (
            {
                "RESOLVER_MODE": "observe",
                "SUPABASE_URL": "https://project.supabase.co",
            },
            "SUPABASE_SERVICE_ROLE_KEY",
        ),
        (
            {
                "RESOLVER_MODE": "observe",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "key",
                "FAILURE_EVENTS_TABLE": "failure_events; drop table",
            },
            "PostgreSQL identifier",
        ),
    ],
)
def test_settings_reject_unsafe_or_incomplete_configuration(environment, error):
    with pytest.raises(ValueError, match=error):
        ObserverSettings.from_env(environment)


def test_safe_failure_event_does_not_retain_failure_body():
    event = SafeFailureEvent.from_realtime_payload(
        {
            "data": {
                "type": "INSERT",
                "commit_timestamp": "2026-07-28T12:00:00Z",
                "record": {
                    "id": "row-1",
                    "failure_id": "failure-1",
                    "sysid": "BILLIE-16",
                    "flow_id": "flow-1",
                    "matcher_status": "pending",
                    "failure_description": "private failure body",
                    "robot_errors": [{"message": "private robot error"}],
                },
            }
        }
    )

    assert event.failure_id == "failure-1"
    assert "private" not in repr(event)


def test_debug_logging_keeps_supabase_dependencies_at_warning(monkeypatch):
    original_levels = {
        name: logging.getLogger(name).level
        for name in _SENSITIVE_DEPENDENCY_LOGGERS
    }
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    try:
        _configure_logging()
        assert all(
            logging.getLogger(name).getEffectiveLevel() >= logging.WARNING
            for name in _SENSITIVE_DEPENDENCY_LOGGERS
        )
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)


@pytest.mark.asyncio
async def test_observer_subscribes_to_insert_and_update_and_shuts_down(caplog):
    client = FakeClient(
        rows=[
            {
                "failure_id": "failure-1",
                "sysid": "BILLIE-16",
                "flow_id": "flow-1",
                "matcher_status": "pending",
                "failure_description": "query must not select this",
            }
        ]
    )

    async def client_factory(url, key):
        assert url == "https://project.supabase.co"
        assert key == "service-role-secret"
        return client

    observer = SupabaseFailureObserver(observer_settings(), client_factory)
    caplog.set_level(logging.INFO, logger="failure_resolver.observer")
    task = asyncio.create_task(observer.run())
    await wait_until(lambda: observer.state.connected)

    assert set(client.channel_instance.bindings) == {"INSERT", "UPDATE"}
    for binding in client.channel_instance.bindings.values():
        assert binding["schema"] == "public"
        assert binding["table"] == "failure_events"

    client.channel_instance.emit(
        "UPDATE",
        {
            "data": {
                "type": "UPDATE",
                "commit_timestamp": "2026-07-28T12:00:00Z",
                "record": {
                    "id": "row-1",
                    "failure_id": "failure-1",
                    "sysid": "untrusted-payload-value",
                    "failure_description": "do not log this private description",
                },
            }
        },
    )

    assert observer.state.events_observed == 1
    await wait_until(lambda: observer.state.rows_fetched == 1)
    assert len(client.queries) == 1
    assert client.queries[0].table == "failure_events"
    assert client.queries[0].columns == "failure_id,sysid,flow_id,matcher_status"
    assert client.queries[0].filter == ("failure_id", "failure-1")
    assert client.queries[0].limit_value == 1
    assert "failure-1" in caplog.text
    assert "BILLIE-16" in caplog.text
    assert "untrusted-payload-value" not in caplog.text
    assert "do not log" not in caplog.text
    assert "query must not select this" not in caplog.text

    await observer.stop()
    await task

    assert client.remove_calls == 1
    assert client.realtime.close_calls == 1
    assert observer.state.running is False
    assert observer.state.connected is False


@pytest.mark.asyncio
async def test_observer_retries_without_logging_exception_details(caplog):
    client = FakeClient()
    calls = 0

    async def client_factory(url, key):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("wss://project.supabase.co?apikey=must-not-leak")
        return client

    observer = SupabaseFailureObserver(observer_settings(), client_factory)
    caplog.set_level(logging.WARNING, logger="failure_resolver.observer")
    task = asyncio.create_task(observer.run())
    await wait_until(lambda: observer.state.connected)

    assert calls == 2
    assert observer.state.reconnect_attempts == 1
    assert "OSError" in caplog.text
    assert "must-not-leak" not in caplog.text

    await observer.stop()
    await task


@pytest.mark.asyncio
async def test_health_is_live_and_ready_only_when_subscribed():
    client = FakeClient()

    async def client_factory(url, key):
        return client

    application = create_app(
        settings=observer_settings(),
        client_factory=client_factory,
    )
    async with application.router.lifespan_context(application):
        await wait_until(lambda: application.state.observer.state.connected)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as http_client:
            health = await http_client.get("/health")
            ready = await http_client.get("/readyz")

            assert health.status_code == 200
            assert health.json()["status"] == "connected"
            assert ready.status_code == 200
            assert ready.json()["connected"] is True

            client.realtime.is_connected = False
            await wait_until(
                lambda: not application.state.observer.state.connected
            )
            reconnecting_health = await http_client.get("/health")
            not_ready = await http_client.get("/readyz")
            assert reconnecting_health.status_code == 200
            assert not_ready.status_code == 503
            assert not_ready.json()["connected"] is False

            application.state.observer.state.running = False
            stopped_health = await http_client.get("/health")
            assert stopped_health.status_code == 503
            application.state.observer.state.running = True
