# Failure Resolver - Setup Guide

## Overview

**Failure Resolver** is an autonomous failure detection, analysis, and resolution system for Bellboy robots. It monitors failures from Avidor (via Supabase), searches semantic memory for similar cases, analyzes with GPT-4, executes solutions on the robot, and learns from operator feedback recorded by Sandy's UI.

## Architecture (Supabase-Based)

```
Failure Detector
  ↓ (inserts row → Supabase failures table)
Supabase
  ├─ failures table (new failure entries)
  └─ solutions table (operator-recorded fixes)
  ↓
Failure Resolver (polls Supabase)
  
  [On New Failure]
  ├─ Extract: robot_id, failure_story, context
  ├─ Search memory: similar failures (Qdrant)
  ├─ Solution found?
  │   ├─ YES → Execute commands on robot (Bellboy API)
  │   └─ NO → Flag for operator
  └─ Store: result + analysis
  
  [On New Solution]
  ├─ Index solution (embeddings + disk)
  ├─ Update metadata
  ├─ Store in failure-resolver-database repo
  └─ Update Supabase with solution link
  
  ↓ (Memory updated → next similar failure has solution)

Billie-UI
  ├─ Reads failures from Supabase
  ├─ Shows failure to operator (if no auto-fix)
  ├─ Operator drives robot manually
  ├─ Records fix as command list
  └─ Inserts to Supabase solutions table (with reference to failure entry)
```

**Benefits:**
- ✅ Decoupled: Services via shared Supabase
- ✅ Scalable: Multiple resolvers can poll independently
- ✅ Observable: Full audit trail in Supabase
- ✅ Autonomous: Executes solutions automatically, learns from operator fixes

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| **API Framework** | FastAPI (Python) | Service, Supabase polling, endpoints |
| **Robot Control** | Bellboy HTTP API | Commands to physical robots |
| **Solution Execution** | SolutionExecutor | Parse and execute command strings |
| **LLM** | GPT-4 (OpenAI) | Failure analysis and reasoning |
| **Semantic Search** | Qdrant vector DB | Embeddings-based failure matching |
| **Embeddings** | sentence-transformers | Generate failure vectors |
| **Memory** | failure-resolver-database (separate repo) | Persistent failure + solution storage |
| **Container** | Docker + Docker Compose | Local dev, cloud deployment |
| **Database** | Supabase (PostgreSQL) | Real-time failure/solution records |

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- Bellboy account (API key)
- Supabase project (database + API)

### Run Locally

```bash
# 1. Clone and configure
git clone https://github.com/bellboy-robotics/failure-resolver.git
cd failure-resolver

# 2. Copy environment template
cp .env.example .env

# 3. Edit .env with:
# - BELLBOY_API_KEY (from Bellboy account)
# - OPENAI_API_KEY (from OpenAI)
# - ROBOT_SYSID (e.g., BILLIE-16) - for unit tests only
# - Supabase credentials (when integrated)

# 4. Start service
docker-compose up --build

# 5. Test robot interface
python3 tests/test_commands.py

# 6. Test memory system
python3 tests/test_memory_management.py

# 7. Import failure data
docker exec failure-resolver python3 import_failures.py failures.csv
```

## Data Flow

### Failure Detection → Analysis

**Failure Detector inserts to Supabase:**
```json
{
  "robot_id": "BILLIE-16",
  "failure_story": "Current map is expected to be floor4 but is osnn_bl",
  "context": "Navigation to floor4 failed during routine delivery",
  "robot_state": {
    "position": [1.2, 5.6, 3.1],
    "status": "stuck"
  },
  "timestamp": "2026-07-28T12:00:00Z"
}
```

**Failure Resolver processes:**
1. Polls Supabase for new failures (configurable interval)
2. Extracts robot_id, failure_story, context
3. Generates embedding (sentence-transformers)
4. Searches Qdrant for similar failures (semantic search)
5. Passes to GPT-4 for analysis with context
6. Stores result + analysis locally
7. Marks failure as "analyzed" in Supabase

### Solution Execution

**If solution exists:**
1. Extract commands from memory (e.g., `slide_forward(0.5)`)
2. Parse with SolutionExecutor
3. Execute on robot via Bellboy API (`POST /robots/{SYSID}/commands`)
4. Report success/failure back to Supabase

**If no solution or operator override:**
1. Flag for operator review
2. Sandy's UI shows failure + context
3. Operator manually drives robot
4. Sandy records solution commands

### Solution Learning

**Billie-UI inserts to Supabase (after operator records solution):**
```json
{
  "robot_id": "BILLIE-16",
  "failure_id": "failure_d02819d9",
  "solution_commands": [
    "slide_forward(0.5)",
    "verify_stability()"
  ],
  "operator_notes": "Changed map to floor4, navigation successful",
  "success": true,
  "timestamp": "2026-07-28T12:05:00Z"
}
```

**Failure Resolver processes:**
1. Generate embedding for solution
2. Store to disk + Qdrant
3. Update metadata index
4. Push to failure-resolver-database repo (develop branch for testing, main for production)
5. Update Supabase with "indexed" status

---

## Memory Structure

```
memory/ (local)
├── index.json
│   └── Metadata: failure_id, command, error, location, etc.
└── failures/
    ├── failure_d02819d9.md  # Failure description
    ├── failure_ee891806.md
    └── ... (human-readable, version-controlled)

failure-resolver-database/ (separate repo)
├── develop/
│   ├── index.json          # Test failure metadata
│   └── failures/*.md       # Test failure cases
└── main/
    ├── index.json          # Production failure metadata
    └── failures/*.md       # Production failure cases
```

**Why separate repos:**
- Code commits don't interlace with data backups
- Database can be managed independently
- Easy to version control both code and data
- Supports test (develop) vs production (main) data

---

## Environment Variables

See `.env.example`:

**Required - Bellboy Robot:**
```
BELLBOY_API_KEY=<api-key-from-bellboy>
ROBOT_SYSID=BILLIE-16              # For unit testing only
```

**Required - LLM:**
```
OPENAI_API_KEY=sk-<your-key>
```

**Qdrant Vector Database:**
```
QDRANT_HOST=qdrant
QDRANT_PORT=6333
```

**Local Storage:**
```
MEMORY_DIR=./memory
DATABASE_URL=sqlite:///./memory/metadata.db
```

**Supabase (When Integrated):**
```
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_KEY=<your-api-key>
FAILURES_TABLE=robot_failures
SOLUTIONS_TABLE=robot_solutions
POLL_INTERVAL_SECONDS=5
```

**Service:**
```
LOG_LEVEL=INFO
PORT=8000
```

---

## Running Tests

```bash
# Memory management (no robot required)
python3 tests/test_memory_management.py

# Robot command execution (requires BELLBOY_API_KEY)
python3 tests/test_commands.py

# Search functionality (no robot required)
python3 tests/test_search_success_fail.py
```

## API Endpoints (Direct - Legacy)

For direct integration before Supabase wiring:

**POST /analyze-failure**
```json
{
  "failure_story": "Current map is expected to be floor4 but is osnn_bl",
  "robot_id": "BILLIE-16"
}
```

**POST /index-solution**
```json
{
  "failure_id": "failure_d02819d9",
  "solution_commands": ["slide_forward(0.5)"],
  "operator_notes": "Manual fix"
}
```

---

## Integration Checklist

- [ ] Supabase project created with failures + solutions tables
- [ ] Failure Detector configured to insert failures into Supabase
- [ ] BELLBOY_API_KEY configured in .env
- [ ] OPENAI_API_KEY configured in .env
- [ ] `docker-compose up` runs without errors
- [ ] Robot interface test passes (`test_commands.py`)
- [ ] Memory test passes (`test_memory_management.py`)
- [ ] Failure data imported (`import_failures.py`)
- [ ] Supabase polling integrated in main.py (failures + solutions)
- [ ] Billie-UI wired to Supabase solutions table
- [ ] Solution execution tested on actual robot

---

## Deployment

```bash
# Build production image
docker build -t failure-resolver:latest .

# Push to registry (GCP, AWS, etc.)
docker tag failure-resolver:latest gcr.io/your-project/failure-resolver:latest
docker push gcr.io/your-project/failure-resolver:latest

# Deploy (Cloud Run, Kubernetes, etc.)
# Ensure Supabase credentials are in production .env
```

---

## Next Steps

1. ✅ Read this file
2. ⬜ Wire up Supabase (failures + solutions tables)
3. ⬜ Test robot connection (`test_commands.py`)
4. ⬜ Import failure database
5. ⬜ Integrate Avidor → Supabase insertion
6. ⬜ Integrate Sandy's UI → solution recording
7. ⬜ Run end-to-end test with actual robot
8. ⬜ Deploy to production

**For more details:** See README.md, BRANCHING_STRATEGY.md, DEVELOPMENT_WORKFLOW.md
