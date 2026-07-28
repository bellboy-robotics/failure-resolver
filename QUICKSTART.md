# Failure Resolver - Quick Start

Your role: **Receive failure stories → Analyze with AI → Run solution on robot**

## 1. Load Initial Failures (One-time)

```bash
cd /Volumes/ws/billie1/failure-resolver

# Import CSV failures into memory
python import_failures.py 10_flow_failures.csv

# Output:
# → Importing failure_d0281... | Current map is expected to be...
# ✓ Added 10 failures to Qdrant
# ✓ Imported 9 failures
```

This loads all failures into:
- **Qdrant** (vector DB for semantic search)
- **./memory/failures/** (markdown files)
- **./memory/index.json** (metadata)

## 2. Receive Failure from Avidor

When robot fails, Avidor sends to SQS:

```json
{
  "robot_id": "billie-001",
  "table_entry_id": 12345,
  "failure_story": "Current map is expected to be `floor4` but is `osnn_bl`",
  "robot_state": {
    "current_map": "osnn_bl",
    "expected_map": "floor4"
  }
}
```

## 3. Failure Resolver Analyzes

```
failure-resolver polls SQS every 5 seconds
  ↓
Searches memory: "Current map is expected..." 
  ↓
Finds similar: "Current map is expected to be `floor4` but is `osnn_bl`"
  ↓
Proposes solution:
  - navigate(floor4)
  - recalibrate_map()
  - verify_stability()
```

Check logs:
```bash
docker logs failure-resolver -f
```

## 4. Execute Solution on Robot

```python
from solution_executor import SolutionExecutor

executor = SolutionExecutor(robot_interface=your_robot_api)

# Validate first (dry-run)
result = await executor.execute_solution(
    commands=[
        "navigate(floor4)",
        "recalibrate_map()",
        "verify_stability()"
    ],
    dry_run=True
)
# Output: {"status": "dry_run_ok", "commands": [...]}

# Execute for real
result = await executor.execute_solution(
    commands=[
        "navigate(floor4)",
        "recalibrate_map()",
        "verify_stability()"
    ]
)
# Output: {"status": "completed", "successful": 3, "commands": [...]}
```

## 5. Record Solution for Future

Once operator fixes it, Sandy's UI sends to SQS:

```json
{
  "robot_id": "billie-001",
  "table_entry_id": 12345,
  "failure_id": "failure_d0281",
  "solution_commands": [
    "navigate(floor4)",
    "recalibrate_map()",
    "verify_stability()"
  ],
  "operator_notes": "Changed map from osnn_bl to floor4 manually",
  "success": true
}
```

failure-resolver:
- Indexes solution to memory
- Embeds it for semantic search
- Next time: **automatically proposes this solution** ✅

---

## Solution Executor API

### Parse & Validate (before running)
```python
commands = [
    "navigate(floor4)",
    "reduce_damping(0.5)",
    "reset_joint(3)"
]

parsed = executor.parse_solution(commands)
# → [SolutionCommand, SolutionCommand, ...]

is_valid = executor.validate_solution(parsed)
# → True if all commands are recognized
```

### Execute with Error Handling
```python
result = await executor.execute_solution(commands, dry_run=False)

# Check result
if result["status"] == "completed":
    print(f"✓ {result['successful']}/{result['total']} commands succeeded")
    for cmd in result["commands"]:
        if cmd["status"] == "failed":
            print(f"✗ {cmd['command']}: {cmd['error']}")
```

### Get Help
```python
executor.get_command_help()
# {
#   "navigate": "Navigate to POI - navigate(poi_name)",
#   "move_arm": "Move arm - move_arm(x, y, z)",
#   ...
# }
```

---

## Architecture Summary

```
CSV Failures (10_flow_failures.csv)
  ↓ (import_failures.py)
Qdrant (semantic search) + Disk (markdown files)
  ↓
failure-resolver polls SQS
  ├─ Receives failure from Avidor
  ├─ Searches memory for similar failures + solutions
  ├─ Proposes best solution
  └─ Stores in local cache
  ↓
You execute solution with SolutionExecutor
  ├─ Validates commands
  ├─ Executes on robot
  └─ Gets results
  ↓
Sandy's UI records result to SQS
  ↓
failure-resolver indexes solution
  ↓
Memory grows → future failures get faster solutions ✅
```

---

## Customization

### Add Custom Commands
Edit `solution_executor.py`:

```python
def _get_available_commands(self) -> List[str]:
    return [
        "your_command_1",
        "your_command_2",
        # ...
    ]
```

### Connect to Robot API
```python
async def your_robot_api(command_name: str, **args):
    """Connect to your robot's actual API."""
    if command_name == "navigate":
        return await robot.navigate(args.get("value"))
    elif command_name == "move_arm":
        return await robot.move_arm(**args)
    # ...

executor = SolutionExecutor(robot_interface=your_robot_api)
```

### Use Real Database
Replace local cache with your DB (Supabase, etc.) for storing/querying failures + solutions.

---

## Next Steps

1. ✅ Import CSV failures
2. ⬜ Connect Avidor's SQS messages
3. ⬜ Implement robot API integration
4. ⬜ Test failure → solution → execution flow
5. ⬜ Record operator solutions to SQS
