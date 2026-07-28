# End-to-End Test Guide

Complete flow: CSV failures → AI analysis → Robot execution

## Prerequisites

1. **Bellboy API key** (get from your Bellboy account settings)
2. **Robot sysid** (e.g., `BILLIE-10` - MUST BE UPPERCASE)
3. **Python dependencies**:
   ```bash
   pip install httpx sentence-transformers openai qdrant-client boto3
   ```

---

## Step 1: Set Up API Key

Add to `.env` file:
```bash
cd /Volumes/ws/billie1/failure-resolver
echo "BELLBOY_API_KEY=your-api-key-here" >> .env
echo "ROBOT_SYSID=BILLIE-10" >> .env
```

Or export in terminal:
```bash
export BELLBOY_API_KEY="your-api-key"
export ROBOT_SYSID="BILLIE-10"
```

---

## Step 2: Import Failures into Memory

```bash
cd /Volumes/ws/billie1/failure-resolver

python import_failures.py 10_flow_failures.csv
```

**Output:**
```
→ Importing failure_d0281: navigate_poi | Current map is expected...
→ Importing failure_ee891: replay_policy | Base pose proximity exceeded...
...
✓ Imported 9 failures
✓ Added 9 failures to Qdrant
✓ Metadata saved to ./memory/index.json
✓ Failure files saved to ./memory/failures/
```

---

## Step 3: Test Robot Connection

```bash
python robot_interface.py
```

**Output:**
```
Robot interface ready for BILLIE-10
Sending: slide forward at 0.5m
✓ Command queued: slide
Sending: twist right by 45 steps
✓ Command queued: twist
...
```

---

## Step 4: Test Solution Execution

```bash
python test_solution_execution.py
```

**Output:**
```
============================================================
FAILURE RESOLVER - Solution Execution Test
============================================================

[1/4] Initializing robot interface for billie-10...
[2/4] Connecting to robot...
✓ Connected to billie-10
[3/4] Creating solution executor...
[4/4] Executing test solution 1: Navigate and verify...

Solution: 
[
  "navigate(floor4)",
  "verify_stability()"
]

→ Validating solution (dry-run)...
Validation result: dry_run_ok

→ Executing solution on robot...
Executing: navigate(floor4)
✓ navigate(floor4) succeeded
Executing: verify_stability()
✓ verify_stability() succeeded

============================================================
EXECUTION RESULTS
============================================================
Status: completed
Commands executed: 2
Successful: 2

✓ navigate(floor4)
  Status: success
  Response: Navigation to floor4 complete
✓ verify_stability()
  Status: success
  Response: Stability verified

============================================================
Disconnecting from robot...
✓ Test complete
```

---

## Step 4: Full Integration Test

### Scenario: Robot reports navigation failure

**1. Simulate failure in SQS:**

```bash
# In a separate terminal, send test message to SQS
aws sqs send-message \
  --queue-url <FAILURES_QUEUE_URL> \
  --message-body '{
    "robot_id": "billie-10",
    "table_entry_id": 100,
    "failure_story": "Current map is expected to be floor4 but is osnn_bl",
    "context": "Navigation to floor4 failed"
  }' \
  --region us-east-1
```

**2. Watch failure-resolver analyze:**

```bash
docker logs failure-resolver -f
```

**Output:**
```
INFO: SQS: Processing failure for robot billie-10
INFO: Analyzing failure: Current map is expected to be floor4...
INFO: Found 1 similar failures (similarity: 0.92)
INFO: Proposed solution:
  - navigate(floor4)
  - verify_stability()
INFO: Solution indexed - billie-10:100
```

**3. Execute proposed solution:**

```python
from robot_interface import RobotInterface
from solution_executor import SolutionExecutor
import asyncio
import os

async def execute():
    robot = RobotInterface(
        sysid=os.getenv("ROBOT_SYSID"),
        token=os.getenv("ROBOT_TOKEN")
    )
    
    await robot.connect()
    
    executor = SolutionExecutor(
        robot_interface=robot.execute_solution_command
    )
    
    solution = [
        "navigate(floor4)",
        "verify_stability()"
    ]
    
    result = await executor.execute_solution(solution)
    
    print(f"✓ {result['successful']}/{result['total']} commands succeeded")
    
    await robot.disconnect()

asyncio.run(execute())
```

**4. Record result (Sandy's UI):**

```bash
aws sqs send-message \
  --queue-url <SOLUTIONS_QUEUE_URL> \
  --message-body '{
    "robot_id": "billie-10",
    "table_entry_id": 100,
    "failure_id": "failure_d0281",
    "solution_commands": [
      "navigate(floor4)",
      "verify_stability()"
    ],
    "operator_notes": "Changed map to floor4, navigation successful",
    "success": true
  }' \
  --region us-east-1
```

**5. Verify solution indexed:**

```bash
docker logs failure-resolver -f
```

**Output:**
```
INFO: SQS: Processing solution for robot billie-10
INFO: Indexed solution to memory
INFO: Solution ready for future similar failures
```

---

## Troubleshooting

### Robot connection fails
```
✗ Connection failed: [Errno -2] Name or service not known
```
- Check ROBOT_TOKEN is valid (not expired)
- Check ROBOT_SYSID is correct
- Verify robot is online

### Solution validation fails
```
validation_failed: One or more commands failed validation
```
- Check command names are in `solution_executor.py`'s `_get_available_commands()`
- Verify command syntax: `command_name(arg1, arg2)`

### SQS messages not processed
```
docker logs failure-resolver | grep "SQS"
```
- Check AWS credentials are set
- Verify queue URLs are correct
- Ensure `POLL_INTERVAL_SECONDS` is set

---

## Commands Supported

From `robot_interface.py` (maps to Bellboy API):

| Solution Command | Robot API | Example |
|---|---|---|
| `slide(direction, meters)` | `slide` | `slide(forward, 1.5)` |
| `slide_forward(meters)` | `slide` | `slide_forward(0.5)` |
| `slide_backward(meters)` | `slide` | `slide_backward(0.5)` |
| `twist(direction, steps)` | `twist` | `twist(right, 45)` |
| `twist_left(steps)` | `twist` | `twist_left(90)` |
| `twist_right(steps)` | `twist` | `twist_right(90)` |
| `abort()` | `abort` | `abort()` |
| `dock()` | `dock` | `dock()` |
| `wait(seconds)` | sleep | `wait(2)` |
| `verify_stability()` | mock | `verify_stability()` |

Add more commands by extending `RobotInterface.execute_solution_command()`.

**API Details:**
- Base URL: `https://api.bellboy.co/robots/{SYSID}/commands`
- Auth: `Authorization: {BELLBOY_API_KEY}`
- Response: `{"pending": true}` = success (command queued)
- Commands execute asynchronously on robot

---

## Next Steps

- [ ] Import failures
- [ ] Get robot token
- [ ] Test robot connection
- [ ] Test solution execution
- [ ] Integrate with Avidor (SQS failures)
- [ ] Integrate with Sandy's UI (SQS solutions)
- [ ] Monitor memory growth (success rates)
