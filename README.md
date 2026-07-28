# Failure Resolver

The default container is currently a read-only Supabase observer. It subscribes
to `INSERT` and `UPDATE` changes on `public.failure_events`, then fetches and
logs only `failure_id`, `sysid`, `flow_id`, and `matcher_status`. It does not
invoke a model, Qdrant, SQS, or robot actions.

## Quick Start

```bash
cp .env.example .env
docker compose up --build
```

Required server-side values:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Optional values are `FAILURE_EVENTS_TABLE` (default `failure_events`),
`LOG_LEVEL` (default `INFO`), and `PORT` (default `8000`).

The service exposes:

- `GET /health` — liveness and connection state
- `GET /readyz` — HTTP 200 only while the Realtime subscription is connected

The database table must be included in the Supabase `supabase_realtime`
publication. The service-role key is a server secret and must never be placed in
a browser or committed to this repository.

The previous agent/memory prototype remains in `main.py` and related modules,
but it is not imported by the observer container. See [SETUP.md](./SETUP.md) for
that legacy prototype.
