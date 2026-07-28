# Integration Guide - Billie Memory Service

## For Avidor's Failure Detection Service

### 1. Create SQS Queue
```bash
aws sqs create-queue --queue-name billie-failures --region us-east-1
# Returns: QueueUrl = https://sqs.us-east-1.amazonaws.com/123456789/billie-failures
```

### 2. When Failure Detected
Send message to SQS:

```python
import boto3
import json

sqs = boto3.client('sqs', region_name='us-east-1')

sqs.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789/billie-failures',
    MessageBody=json.dumps({
        'robot_id': 'billie-001',           # Required: robot identifier
        'table_entry_id': 12345,            # Required: DB record ID
        'failure_story': 'Robot arm oscillating at joint 3...',  # Required
        'robot_state': {                    # Optional but recommended
            'arm_position': [0.5, 1.2, 0.8],
            'gripper_pressure': 80,
            'status': 'failed'
        },
        'context': 'Attempted to pick object during run_id=abc123'  # Optional
    })
)
```

### 3. Billie Memory Processes
- Polls failures queue every 5 seconds
- Analyzes failure with GPT-4
- Searches memory for similar failures + solutions
- Stores result locally (keyed by `robot_id:table_entry_id`)
- Deletes message from queue

### 4. Query Results
Billie Memory stores results in local cache:
```
memory/index.json  # Metadata index
```

You can read the result from Billie Memory's local storage or implement a callback queue for responses.

---

## For Sandy's Operator UI Service

### 1. Create SQS Queue
```bash
aws sqs create-queue --queue-name billie-solutions --region us-east-1
# Returns: QueueUrl = https://sqs.us-east-1.amazonaws.com/123456789/billie-solutions
```

### 2. When Operator Records Solution
After operator drives robot and records solution commands, send to SQS:

```python
import boto3
import json

sqs = boto3.client('sqs', region_name='us-east-1')

sqs.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789/billie-solutions',
    MessageBody=json.dumps({
        'robot_id': 'billie-001',           # Required: match failure's robot_id
        'table_entry_id': 12345,            # Required: match failure's table_entry_id
        'failure_id': 'failure_001',        # Required: identifier for this failure type
        'solution_commands': [              # Required: list of commands that fixed it
            'reduce_damping(0.5)',
            'reset_joint(3)',
            'verify_stability()'
        ],
        'operator_notes': 'Operator reduced damping coefficient from 1.0 to 0.5 to eliminate oscillation',  # Required
        'success': True                     # Required: did it work?
    })
)
```

### 3. Billie Memory Processes
- Polls solutions queue every 5 seconds
- Embeds solution with sentence-transformers
- Stores to disk (markdown files)
- Adds to Qdrant vector store for semantic search
- Updates metadata index
- Deletes message from queue

### 4. Next Similar Failure
When same/similar failure occurs, Billie Memory will:
- Find this solution in memory
- Propose it automatically
- Reduce need for operator intervention

---

## Billie Memory Configuration

Update `.env` with your SQS queue URLs:

```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
FAILURES_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/billie-failures
SOLUTIONS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/billie-solutions
POLL_INTERVAL_SECONDS=5
```

Then restart:
```bash
docker-compose down
docker-compose up -d
```

---

## Local Testing

### Test Failure Message
```bash
aws sqs send-message \
  --queue-url https://sqs.us-east-1.amazonaws.com/123456789/billie-failures \
  --message-body '{
    "robot_id": "billie-001",
    "table_entry_id": 100,
    "failure_story": "Gripper pressure too high, unable to grip objects",
    "context": "During pick attempt"
  }' \
  --region us-east-1
```

### Check Billie Memory Logs
```bash
docker logs billie-memory-service -f
# Should see: "SQS: Processing failure for robot billie-001"
```

---

## What Billie Memory Does NOT Handle (Yet)

- **Response queue**: Results stored locally. Implement a callback mechanism if needed.
- **Failure history**: DB schema needed. Currently stores in local JSON cache.
- **Solution statistics**: Track success rates post-hackathon.
- **Operator feedback**: Whether proposed solution was good/bad.

These can be added post-hackathon when architecture is settled.
