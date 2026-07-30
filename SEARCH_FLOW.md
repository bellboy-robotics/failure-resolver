# Cabinet-Style Memory Retrieval

The supported resolver uses a Markdown-first memory design:

- each successful demonstrated recovery is one Markdown document;
- Git is the durable source of truth and audit history;
- the database supplies complete failure and resolution evidence;
- a deterministic in-memory index is rebuilt from the Git checkout; and
- the model may search, read, refine, and select an exact memory ID.

The legacy Qdrant prototype is not part of the default resolver runtime.

## What a memory contains

Each `memories/<resolution_id>.md` document contains:

- retrieval metadata such as robot, site, room, map, flow, item, and command;
- generalized failure and recovery prose;
- a structured retrieval signature;
- the complete sanitized failure and resolution episode; and
- the exact successful dispatched actions.

Credential-like fields, operator e-mail, and resolver bookkeeping are removed
before the episode is written. Oversized evidence fails ingestion rather than
being silently truncated.

The `## Dispatched Actions` section is authoritative executable data. It is
written by application code from the successful resolution record, not by the
model.

## Matching a new failure

1. The resolver claims a pending `failure_events` row.
2. It pulls the memory branch and parses all Markdown documents.
3. Unsafe or non-actionable memories are removed.
4. It builds one immutable search snapshot for that failure.
5. For up to four small memories, it reads the complete safe corpus and asks
   the model for an exact ID or `no_solution`.
6. For a larger corpus, the model operates a bounded loop:
   - `search(query)` ranks the full corpus;
   - `read(memory_ids)` opens exact IDs returned by search;
   - `finish(choice)` selects one read ID or returns `no_solution`.
7. Application code validates the selected ID against the same immutable
   snapshot and copies the exact stored actions into `resolver_suggestion`.

A model cannot select an unread ID, invent an ID, provide action parameters, or
read arbitrary paths. Search/read counts and cumulative content have hard
bounds; malformed or over-budget behavior fails closed.

## Search ranking

`retrieval.py` performs deterministic weighted full-text ranking across:

| Field | Weight |
| --- | ---: |
| Retrieval signature | 12 |
| Frontmatter metadata | 8 |
| Generalized prose | 6 |
| Complete episode evidence | 4 |

Exact metadata hints boost matches for robot, site, floor, room, map, flow,
activity, area, item, and failed command. Hints improve ranking but never
exclude a document. Ties are stable by memory ID, and results include bounded
snippets so the model can decide which records to read.

This first version intentionally keeps the index local and reproducible from
Git. A vector retriever can later be added as another candidate generator
without changing Markdown authority, read-before-select validation, or exact
action handling.

## Learning and durability

When an applied resolution has `outcome=resolved`:

1. the resolver joins it with its linked full failure row;
2. OpenAI produces only generalized prose, tags, and a retrieval signature;
3. application code renders the Markdown with the original successful actions;
4. the file is committed and pushed; and
5. Supabase is acknowledged with the remotely durable Git commit.

If a prior commit succeeded locally but its push failed, reconciliation pushes
that commit before marking the memory as ingested. If improved episode evidence
rewrites an already-ingested memory, the resolver updates the stored commit SHA
with a compare-and-set without regressing the visible ingestion status.
