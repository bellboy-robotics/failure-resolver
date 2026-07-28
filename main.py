import os
import json
import logging
import asyncio
from typing import Optional
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from openai import OpenAI
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
from sqs_handler import SQSHandler

load_dotenv()

# Setup logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Initialize clients
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
qdrant_client = QdrantClient(
    host=os.getenv("QDRANT_HOST", "localhost"),
    port=int(os.getenv("QDRANT_PORT", 6333)),
)
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
sqs_handler = SQSHandler()

# Background tasks
sqs_polling_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup and shutdown."""
    # Startup: start SQS polling tasks
    failure_task = asyncio.create_task(sqs_handler.poll_failures(process_sqs_failure))
    solution_task = asyncio.create_task(
        sqs_handler.poll_solutions(process_sqs_solution)
    )
    sqs_polling_tasks.extend([failure_task, solution_task])
    logger.info("SQS polling tasks started")

    yield

    # Shutdown: cancel polling tasks
    for task in sqs_polling_tasks:
        task.cancel()
    logger.info("SQS polling tasks stopped")


# Initialize FastAPI with lifespan
app = FastAPI(title="Billie Memory Service", version="0.1.0", lifespan=lifespan)

# Memory directory
MEMORY_DIR = Path(os.getenv("MEMORY_DIR", "./memory"))
FAILURES_DIR = MEMORY_DIR / "failures"
FAILURES_DIR.mkdir(parents=True, exist_ok=True)

# Collection name for Qdrant
COLLECTION_NAME = "failures"


# Pydantic models
class RobotState(BaseModel):
    arm_position: Optional[list] = None
    gripper_pressure: Optional[float] = None
    status: Optional[str] = None


class AnalyzeFailureRequest(BaseModel):
    failure_story: str
    robot_state: Optional[RobotState] = None
    context: Optional[str] = None


class IndexSolutionRequest(BaseModel):
    failure_id: str
    solution_commands: list[str]
    operator_notes: str
    success: bool


class SolutionResponse(BaseModel):
    id: str
    commands: list[str]
    confidence: float
    notes: str


class AnalyzeFailureResponse(BaseModel):
    matches: list[dict] = []
    proposed_solution: Optional[SolutionResponse] = None
    escalate: bool = True
    reasoning: str = ""


# Health check
@app.get("/health")
async def health():
    return {"status": "ok"}


# Main endpoint: Analyze failure
@app.post("/analyze-failure", response_model=AnalyzeFailureResponse)
async def analyze_failure(request: AnalyzeFailureRequest):
    """
    Analyze a failure story from Avidor.
    Search memory for similar failures + solutions.
    """
    logger.info(f"Analyzing failure: {request.failure_story[:100]}...")

    try:
        # Step 1: Embed the failure story
        failure_embedding = embedding_model.encode(request.failure_story).tolist()

        # Step 2: Search Qdrant for similar failures
        matches = search_similar_failures(failure_embedding)
        logger.info(f"Found {len(matches)} similar failures")

        # Step 3: Use Claude to reason about the failure + solutions
        reasoning = await reason_about_failure(
            failure_story=request.failure_story,
            robot_state=request.robot_state,
            similar_failures=matches,
        )

        # Step 4: Propose solution or escalate
        if matches and matches[0]["similarity"] > 0.7:
            solution = extract_solution(matches[0])
            return AnalyzeFailureResponse(
                matches=matches[:3],  # Return top 3 matches
                proposed_solution=solution,
                escalate=False,
                reasoning=reasoning,
            )
        else:
            return AnalyzeFailureResponse(
                matches=matches[:3],
                escalate=True,
                reasoning=reasoning,
            )

    except Exception as e:
        logger.error(f"Error analyzing failure: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Index new solution
@app.post("/index-solution")
async def index_solution(request: IndexSolutionRequest):
    """
    Index a new solution from operator (via Sandy's service).
    Store failure + solution in memory, embed it for future retrieval.
    """
    logger.info(f"Indexing solution for failure {request.failure_id}")

    try:
        # Step 1: Save solution to disk
        failure_dir = FAILURES_DIR / request.failure_id
        failure_dir.mkdir(exist_ok=True)

        solution_id = f"solution_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        solution_file = failure_dir / f"{solution_id}.md"

        solution_content = f"""# Solution: {request.failure_id}

## Commands
{chr(10).join(f"- {cmd}" for cmd in request.solution_commands)}

## Operator Notes
{request.operator_notes}

## Success
{request.success}

## Indexed At
{datetime.now().isoformat()}
"""
        solution_file.write_text(solution_content)
        logger.info(f"Saved solution file: {solution_file}")

        # Step 2: Embed solution for future retrieval
        solution_text = f"{request.failure_id}\n{request.operator_notes}\n{chr(10).join(request.solution_commands)}"
        solution_embedding = embedding_model.encode(solution_text).tolist()

        # Step 3: Add to Qdrant vector store (if collection exists, use it; else create)
        try:
            # Try to upsert into existing collection
            qdrant_client.upsert(
                collection_name=COLLECTION_NAME,
                points=[
                    {
                        "id": hash(solution_id) % (10**9),  # Use hash as ID
                        "vector": solution_embedding,
                        "payload": {
                            "failure_id": request.failure_id,
                            "solution_id": solution_id,
                            "description": request.operator_notes,
                            "success": request.success,
                            "commands": request.solution_commands,
                            "type": "solution",
                        },
                    }
                ],
            )
            logger.info(f"Added solution to Qdrant: {solution_id}")
        except Exception as e:
            logger.warning(f"Could not add to Qdrant (collection may not exist yet): {str(e)}")

        # Step 4: Store metadata locally
        metadata_file = MEMORY_DIR / "index.json"
        metadata_index = {}
        if metadata_file.exists():
            metadata_index = json.loads(metadata_file.read_text())

        metadata_index[solution_id] = {
            "failure_id": request.failure_id,
            "solution_id": solution_id,
            "commands": request.solution_commands,
            "operator_notes": request.operator_notes,
            "success": request.success,
            "indexed_at": datetime.now().isoformat(),
            "file": str(solution_file),
        }
        metadata_file.write_text(json.dumps(metadata_index, indent=2))

        logger.info(f"Indexed solution {solution_id} for failure {request.failure_id}")
        return {
            "indexed": True,
            "solution_id": solution_id,
            "failure_id": request.failure_id,
            "stored_in": str(solution_file),
            "success": request.success,
        }

    except Exception as e:
        logger.error(f"Error indexing solution: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# SQS Callbacks
async def process_sqs_failure(message: dict) -> bool:
    """Process failure message from SQS."""
    try:
        robot_id = message.get("robot_id")
        table_entry_id = message.get("table_entry_id")
        failure_story = message.get("failure_story")
        robot_state_dict = message.get("robot_state")
        context = message.get("context")

        logger.info(
            f"SQS: Processing failure for robot {robot_id}, entry {table_entry_id}"
        )

        # Create RobotState object if provided
        robot_state = None
        if robot_state_dict:
            robot_state = RobotState(**robot_state_dict)

        # Analyze the failure
        failure_embedding = embedding_model.encode(failure_story).tolist()
        matches = search_similar_failures(failure_embedding)

        reasoning = await reason_about_failure(
            failure_story=failure_story,
            robot_state=robot_state,
            similar_failures=matches,
        )

        # Determine solution
        proposed_solution = None
        escalate = True
        if matches and matches[0]["similarity"] > 0.7:
            proposed_solution = extract_solution(matches[0])
            escalate = False

        # Store result in local cache (in production, store in DB)
        result = {
            "robot_id": robot_id,
            "table_entry_id": table_entry_id,
            "status": "analyzed",
            "matches": matches[:3],
            "proposed_solution": proposed_solution.dict() if proposed_solution else None,
            "escalate": escalate,
            "reasoning": reasoning,
            "analyzed_at": datetime.now().isoformat(),
        }

        # Log result (in production, update DB)
        logger.info(f"SQS: Failure analyzed - {result}")

        return True
    except Exception as e:
        logger.error(f"Error processing SQS failure: {str(e)}")
        return False


async def process_sqs_solution(message: dict) -> bool:
    """Process solution message from SQS."""
    try:
        robot_id = message.get("robot_id")
        table_entry_id = message.get("table_entry_id")
        failure_id = message.get("failure_id")
        solution_commands = message.get("solution_commands")
        operator_notes = message.get("operator_notes")
        success = message.get("success", True)

        logger.info(
            f"SQS: Processing solution for robot {robot_id}, entry {table_entry_id}"
        )

        # Index the solution
        failure_dir = FAILURES_DIR / failure_id
        failure_dir.mkdir(exist_ok=True)

        solution_id = f"solution_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        solution_file = failure_dir / f"{solution_id}.md"

        solution_content = f"""# Solution: {failure_id}

## Commands
{chr(10).join(f"- {cmd}" for cmd in solution_commands)}

## Operator Notes
{operator_notes}

## Success
{success}

## Indexed At
{datetime.now().isoformat()}
"""
        solution_file.write_text(solution_content)

        # Embed solution
        solution_text = f"{failure_id}\n{operator_notes}\n{chr(10).join(solution_commands)}"
        solution_embedding = embedding_model.encode(solution_text).tolist()

        try:
            qdrant_client.upsert(
                collection_name=COLLECTION_NAME,
                points=[
                    {
                        "id": hash(solution_id) % (10**9),
                        "vector": solution_embedding,
                        "payload": {
                            "failure_id": failure_id,
                            "solution_id": solution_id,
                            "description": operator_notes,
                            "success": success,
                            "commands": solution_commands,
                            "type": "solution",
                        },
                    }
                ],
            )
        except Exception as e:
            logger.warning(f"Could not add to Qdrant: {str(e)}")

        # Store metadata
        metadata_file = MEMORY_DIR / "index.json"
        metadata_index = {}
        if metadata_file.exists():
            metadata_index = json.loads(metadata_file.read_text())

        metadata_index[solution_id] = {
            "robot_id": robot_id,
            "table_entry_id": table_entry_id,
            "failure_id": failure_id,
            "solution_id": solution_id,
            "commands": solution_commands,
            "operator_notes": operator_notes,
            "success": success,
            "indexed_at": datetime.now().isoformat(),
            "file": str(solution_file),
        }
        metadata_file.write_text(json.dumps(metadata_index, indent=2))

        logger.info(f"SQS: Solution indexed - {solution_id}")
        return True
    except Exception as e:
        logger.error(f"Error processing SQS solution: {str(e)}")
        return False


# Helper functions
def search_similar_failures(embedding: list[float], limit: int = 5) -> list[dict]:
    """Search Qdrant for similar failures."""
    try:
        # If collection doesn't exist yet, return empty
        results = qdrant_client.search(
            collection_name=COLLECTION_NAME,
            query_vector=embedding,
            limit=limit,
        )
        return [
            {
                "failure_id": result.payload.get("failure_id"),
                "similarity": result.score,
                "description": result.payload.get("description"),
            }
            for result in results
        ]
    except Exception as e:
        logger.warning(f"Search failed (collection may not exist): {str(e)}")
        return []


async def reason_about_failure(
    failure_story: str, robot_state: Optional[RobotState], similar_failures: list
) -> str:
    """Use GPT to reason about the failure."""
    prompt = f"""You are an expert robot diagnostician. Analyze this failure:

Failure Story: {failure_story}

Robot State: {robot_state.dict() if robot_state else "Unknown"}

Similar Past Failures:
{chr(10).join(f"- {f['description']}" for f in similar_failures[:3]) if similar_failures else "None found"}

Provide a concise analysis:
1. Root cause hypothesis
2. Confidence level
3. Recommended next step
"""

    message = openai_client.chat.completions.create(
        model="gpt-4",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.choices[0].message.content


def extract_solution(match: dict) -> SolutionResponse:
    """Extract solution details from a match."""
    failure_id = match["failure_id"]

    # Try to load solution from disk
    failure_dir = FAILURES_DIR / failure_id
    if failure_dir.exists():
        solution_files = list(failure_dir.glob("solution_*.md"))
        if solution_files:
            # Use the most recent solution
            solution_file = sorted(solution_files)[-1]
            try:
                content = solution_file.read_text()
                # Parse commands from markdown
                commands = []
                in_commands = False
                for line in content.split("\n"):
                    if line.strip() == "## Commands":
                        in_commands = True
                        continue
                    if in_commands and line.startswith("- "):
                        commands.append(line[2:].strip())
                    elif in_commands and line.startswith("##"):
                        break

                return SolutionResponse(
                    id=solution_file.stem,
                    commands=commands if commands else ["No commands recorded"],
                    confidence=match["similarity"],
                    notes=f"Solution from failure {failure_id}",
                )
            except Exception as e:
                logger.warning(f"Could not load solution: {str(e)}")

    return SolutionResponse(
        id=f"{failure_id}_solution",
        commands=["No solution found yet"],
        confidence=match["similarity"],
        notes=f"Similar to failure {failure_id} (waiting for operator solution)",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
