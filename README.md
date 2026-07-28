# Billie Memory Service

Failure analysis and solution memory system for Billie the robot.

**See [SETUP.md](./SETUP.md) for architecture and detailed setup instructions.**

## Quick Start

```bash
# Copy env
cp .env.example .env

# Edit .env with your ANTHROPIC_API_KEY
nano .env

# Start service
docker-compose up --build

# Test
curl -X POST http://localhost:8000/analyze-failure \
  -H "Content-Type: application/json" \
  -d '{"failure_story": "Gripper pressure too high"}'
```

## API

- `POST /analyze-failure` — Search memory for similar failures + propose solution
- `POST /index-solution` — Store new solution from operator

See SETUP.md for full API spec.

## Stack

- LangGraph (agent orchestration)
- GPT-4 API (OpenAI) (reasoning)
- Qdrant (semantic search)
- FastAPI (HTTP service)

## Phase 2

Can swap LangGraph for **Hermes Agent** if multi-platform UI needed. Memory structure stays the same.
