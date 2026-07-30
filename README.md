# Failure Resolver

The default container runs `resolver.py`, the online Supabase and Markdown
memory agent. It:

- subscribes to `failure_events` and `flow_failure_resolutions`;
- reconciles pending failures and successful demonstrated resolutions on
  startup, so Realtime is not the durable queue;
- changes a pending failure to `matching`, then `solution_found`,
  `no_solution`, or `failed`;
- rebuilds a deterministic search index from Git-backed Markdown memory;
- lets OpenAI search, read, refine, and select an exact memory ID within hard
  retrieval budgets (small memory sets are read exhaustively);
- generalizes successful operator-demonstrated resolutions into Markdown; and
- commits and pushes those memories to the configured repository.

Automatic robot execution is disabled by default. When explicitly enabled for
an allowlisted robot, a separate coordinator claims a database-backed recovery
session, executes the selected memory's exact demonstrated correction actions
in order, and sends one guarded `$resume_flow` continuation. Search and model
output cannot create or modify executable actions.

## Run locally

```bash
cp .env.example .env
# Fill the required secrets in .env.
docker compose up --build
```

Required server-side values:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `OPENAI_API_KEY`
- `GITHUB_TOKEN`

`GITHUB_TOKEN` needs read/write access only to `MEMORY_REPO_URL`. The image
passes it to Git through `/app/git-askpass.sh`; interactive credential prompts
are disabled. Never commit `.env` or put these credentials in browser code.

The main optional settings are:

- `OPENAI_MODEL` (default `gpt-5.6-luna`)
- `FAILURE_EVENTS_TABLE` (default `failure_events`)
- `FLOW_FAILURE_RESOLUTIONS_TABLE` (default
  `flow_failure_resolutions`)
- `MEMORY_REPO_URL`
- `MEMORY_REPO_BRANCH` (default `failure-resolver-dev`)
- `MEMORY_REPO_ROOT` (default
  `/var/lib/failure-resolver/repository`)
- `RESOLVER_AUTO_EXECUTE` (default `false`)
- `RECOVERY_ROBOT_ALLOWLIST` (required when auto execution is enabled)
- `RECOVERY_MAX_ATTEMPTS` (default `3`)
- `RECOVERY_COMMAND_TIMEOUT_SECONDS` (default `15`)
- `RECOVERY_OUTCOME_TIMEOUT_SECONDS` (default `60`)
- `RECOVERY_LEASE_SECONDS` (default `300`; with auto execution enabled,
  must be at least
  `10 * RECOVERY_COMMAND_TIMEOUT_SECONDS + RECOVERY_OUTCOME_TIMEOUT_SECONDS + 5`)
- `RECOVERY_RECONCILE_INTERVAL_SECONDS` (default `30`, must be shorter
  than the lease)
- `LOG_LEVEL` and `PORT`

Auto execution also requires server-side
`RECOVERY_CF_ACCESS_CLIENT_ID` and `RECOVERY_CF_ACCESS_CLIENT_SECRET`.
When `RESOLVER_AUTO_EXECUTE=true`, build the matching Cloud UI with
`NEXT_PUBLIC_FAILURE_AUTO_RECOVERY_ENABLED=true`; when it is false, keep the
Cloud flag false too. The paired PoC flags reserve suggested fixes for the
automatic worker before its first database claim. A production version should
replace them with one shared database claim for manual and automatic
executors.
Keep it off until the Cloud recovery migration is applied and the target robot
Brain supports `$resume_flow`. A timeout, disconnect, stale Flow pointer, or
ambiguous outcome becomes terminal `unknown`; it is not sent again. A
definitive recurrence of the same failed step consumes another attempt, and
the final failed attempt becomes `timed_out`.

Compose mounts `/var/lib/failure-resolver` as a named volume so the checkout
survives container replacement. Markdown Git history remains the authoritative
memory; the local checkout can be recreated from the remote.

## Health

- `GET /health` — process liveness and resolver counters
- `GET /readyz` — HTTP 200 only while both Supabase Realtime subscriptions are
  connected

The tables must be in the `supabase_realtime` publication. The service-role key
is required because the agent reads both tables and atomically updates matcher
state in `failure_events`.

## Validate

```bash
python -m pip install -r requirements-observer-dev.txt
python -m pytest -q tests/test_retrieval.py tests/test_agent.py \
  tests/test_memory_store.py tests/test_observer.py tests/test_resolver.py \
  tests/test_robot_session.py tests/test_recovery_executor.py
docker compose config
docker build -t failure-resolver:local .
```

The older Qdrant/SQS/robot-execution prototype remains in this repository for
reference, but it is not imported by the default container.
