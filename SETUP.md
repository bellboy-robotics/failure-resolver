# Billie Memory Service - Setup Guide

## Overview

**Billie Memory Service** is the failure analysis and solution memory system. It receives failure stories from Avidor via **AWS SQS**, searches local semantic memory for similar failures and solutions, and proposes fixes or flags for operator intervention.

## Architecture (SQS-Based)

```
Avidor (failure detection)
  ↓ (puts message → failures-queue)
AWS SQS (billie-failures)
  ↓
Billie Memory Service (polls SQS)
  ├─ Parses robot_id, table_entry_id, failure_story
  ├─ Searches memory: failures + solutions
  ├─ Reasons about failure (GPT-4)
  ├─ Stores result in local cache/DB
  └─ Deletes message from queue
  ↓
Sandy's UI (reads failure record from DB/cache)
  ├─ Shows failure to operator
  ├─ Operator drives robot → creates solution
  └─ Puts message → solutions-queue
  ↓
AWS SQS (billie-solutions)
  ↓
Billie Memory Service (polls SQS)
  ├─ Indexes solution (embeddings + disk + metadata)
  ├─ Updates failure record
  └─ Deletes message from queue
  ↓
Memory updated → next similar failure has solution
```

**Benefits:**
- ✅ Robust: Messages not lost if service is down
- ✅ Decoupled: Services don't call each other directly
- ✅ Scalable: Multiple workers can poll same queue
- ✅ Reliable: Built-in retry mechanism

## Technology Stack (Phase 1 - LangGraph)

| Component | Technology | Purpose |
|---|---|---|
| **Agent Framework** | LangGraph (LangChain) | Orchestrate agent logic, memory operations |
| **LLM** | GPT-4 (OpenAI) | Reasoning about failures |
| **Semantic Memory** | Qdrant (local vector DB) | Embeddings-based failure search |
| **Metadata Store** | SQLite / PostgreSQL | Failure logs, solution metadata, statistics |
| **Container** | Docker + Docker Compose | Local dev, eventual cloud deployment |
| **API** | FastAPI (Python) | HTTP endpoints for Avidor, Sandy integration |

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- `ANTHROPIC_API_KEY` environment variable (for Claude API)

### Run Locally

```bash
cd billie-memory-service

# Copy env template
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY

# Build and start
docker-compose up --build

# Test the service
curl -X POST http://localhost:8000/analyze-failure \
  -H "Content-Type: application/json" \
  -d '{"failure_story": "Gripper failed to close, pressure at 120psi"}'
```

## SQS Message Formats

### Failures Queue (billie-failures)
**Message from Avidor:**

```json
{
  "robot_id": "billie-001",
  "table_entry_id": 12345,
  "failure_story": "Robot arm oscillating at joint 3, amplitude increasing",
  "robot_state": {
    "arm_position": [0.5, 1.2, 0.8],
    "gripper_pressure": 80,
    "status": "failed"
  },
  "context": "Attempted to pick object during run_id=abc123"
}
```

**Billie Memory processes this:**
1. Parses robot_id and table_entry_id
2. Analyzes failure_story
3. Searches memory for similar failures + solutions
4. Stores result locally (keyed by robot_id:table_entry_id)
5. Deletes message from queue

**Result stored locally:**
```json
{
  "robot_id": "billie-001",
  "table_entry_id": 12345,
  "status": "analyzed",
  "matches": [...],
  "proposed_solution": {...} or null,
  "escalate": true/false,
  "reasoning": "..."
}
```

---

### Solutions Queue (billie-solutions)
**Message from Sandy's UI (after operator records solution):**

```json
{
  "robot_id": "billie-001",
  "table_entry_id": 12345,
  "failure_id": "failure_001",
  "solution_commands": [
    "reduce_damping(0.5)",
    "reset_joint(3)",
    "verify_stability()"
  ],
  "operator_notes": "Operator reduced damping coefficient from 1.0 to 0.5",
  "success": true
}
```

**Billie Memory processes this:**
1. Embeds solution (sentence-transformers)
2. Stores to disk + Qdrant vector store
3. Updates metadata index
4. Updates failure record in DB
5. Deletes message from queue

---

## REST Endpoints (Optional - for direct integration)

### POST /health
Health check endpoint.

### POST /analyze-failure (Legacy)
Direct HTTP endpoint (for compatibility, but SQS is preferred).

### POST /index-solution (Legacy)
Direct HTTP endpoint (for compatibility, but SQS is preferred).

## Memory Structure

```
memory/
  ├── failures/
  │   ├── failure_001.md          # Failure description + metadata
  │   ├── failure_001_solutions/
  │   │   ├── solution_a.md       # Command stream + operator notes
  │   │   └── solution_b.md       # Alternative solution
  │   └── failure_002.md
  ├── embeddings.db               # Qdrant vector store
  ├── metadata.db                 # SQLite: failure logs, statistics
  └── index.json                  # Embedding IDs, failure ↔ solution links
```

Each failure is text-based (markdown) for:
- ✅ Version control (GitHub backup)
- ✅ Semantic search via embeddings
- ✅ Human-readable logs
- ✅ Easy to extend post-hackathon

## Phase 2 - Future (Optional: Hermes Agent)

If multi-platform operator UI needed (Slack, Discord), can pivot to **Hermes Agent**:
- Keeps same memory structure
- Swaps LangGraph for Hermes orchestration
- Adds platform integrations

For now: **LangGraph is the focus.** Memory structure stays the same.

## Running Tests

```bash
# Unit tests for memory operations
pytest tests/

# Integration test: end-to-end failure → solution
pytest tests/integration/ -v

# Manual test with docker-compose
docker-compose exec agent pytest
```

## Deployment (Post-Hackathon)

```bash
# Build image for cloud
docker build -t billie-memory-service:latest .

# Push to registry (GCP, AWS, or your choice)
docker tag billie-memory-service:latest gcr.io/your-project/billie-memory-service:latest
docker push gcr.io/your-project/billie-memory-service:latest

# Deploy on cloud (Cloud Run, Kubernetes, etc.)
```

## Environment Variables

See `.env.example`:

**LLM & Memory:**
```
OPENAI_API_KEY=sk-...
QDRANT_HOST=qdrant
QDRANT_PORT=6333
DATABASE_URL=sqlite:///./memory/metadata.db
MEMORY_DIR=./memory
LOG_LEVEL=INFO
PORT=8000
```

**AWS SQS:**
```
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
FAILURES_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/billie-failures
SOLUTIONS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/billie-solutions
POLL_INTERVAL_SECONDS=5
```

## Next Steps

1. ✅ Read this file
2. ⬜ Run `docker-compose up` locally
3. ⬜ Test `/analyze-failure` endpoint
4. ⬜ Wire up Avidor → POST failures
5. ⬜ Wire up Sandy's UI → POST solutions
6. ⬜ Build dashboard to visualize memory (failures, solution success rates)

---

**Questions?** Check the inline code comments or ask Claude Code.
