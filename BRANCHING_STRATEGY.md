# Database Branching Strategy

## Branch Structure

```
failure-resolver-database/
├── main
│   └── Production failures (stable, verified)
│       └── Used by: production robots
│       └── Triggers: scheduled backups, tested solutions
│
└── develop
    └── Testing failures (experimental, in-progress)
        └── Used by: test robots, development
        └── Triggers: daily imports, unverified failures
```

---

## Branch Guidelines

### `main` (Production)

**Purpose:** Stable, verified failure database for production robots

**When to commit:**
- ✓ Failures verified and tested
- ✓ Solutions have been validated
- ✓ Data quality reviewed
- ✓ Weekly or after major milestone

**Access:** Restricted to approved deployments

**Commands:**
```bash
# Sync to production
python3 sync_database.py "Production update: validated 50 failures" main

# Or (main is default)
python3 sync_database.py "Production update: validated 50 failures"
```

### `develop` (Testing)

**Purpose:** Experimental failures for testing and development

**When to commit:**
- ✓ New failures from daily imports
- ✓ Testing solutions before production
- ✓ Incomplete or unverified data OK
- ✓ Daily or multiple times per day

**Access:** Open for testing and development

**Commands:**
```bash
# Sync to testing/development
python3 sync_database.py "Daily import: 10 new test failures" develop

# Quick test
python3 sync_database.py "Testing failure resolution on test robot" develop
```

---

## Workflow: Development to Production

```
┌─────────────────────────────────────────────────┐
│  Robot discovers new failure                    │
└────────────────┬────────────────────────────────┘
                 ↓
         import_failures.py
                 ↓
         memory/failures/new.md
                 ↓
    ┌──────────────────────────────┐
    │ Sync to develop branch       │
    │ python3 sync_database.py ... │
    │        "msg" develop         │
    └────────────┬─────────────────┘
                 ↓
         Test on staging robot
         Test failure resolution
         Verify solution works
                 ↓
    ┌──────────────────────────────┐
    │ Merge to main branch         │
    │ git merge develop → main     │
    │ python3 sync_database.py ... │
    │        "msg" main            │
    └────────────┬─────────────────┘
                 ↓
         Deploy to production
         Production robots use main
```

---

## Example Workflow

### Day 1: Testing
```bash
# Import test failures
docker exec failure-resolver python3 import_failures.py test_batch.csv

# Sync to develop (testing branch)
python3 sync_database.py "Test: 5 new failures for validation" develop

# Results:
# https://github.com/bellboy-robotics/failure-resolver-database/tree/develop
```

### Day 2: Validation
```bash
# Test failure resolution on staging robot
# Verify solutions work
# Check for false positives

# If validated, merge to production:
git clone https://github.com/bellboy-robotics/failure-resolver-database.git
cd failure-resolver-database
git checkout main
git merge develop
python3 ../sync_database.py "Validated and merged from develop" main
```

### Day 3: Production
```bash
# Production robots now use main branch
# Latest failures available for resolution
git clone -b main https://github.com/bellboy-robotics/failure-resolver-database.git
```

---

## Data Sync Between Branches

### Option 1: Git Merge (Simple)
```bash
# Promote develop → main
git clone https://github.com/bellboy-robotics/failure-resolver-database.git
cd failure-resolver-database
git checkout main
git merge develop
git push origin main
```

### Option 2: Selective Cherry-Pick (Advanced)
```bash
# Only promote specific failures
git checkout main
git cherry-pick <commit-from-develop>
git push origin main
```

### Option 3: Manual Review
```bash
# Review differences before merge
git diff main develop --stat

# See what changed
git log main..develop

# Merge if approved
git merge develop
```

---

## Docker Deployment

### Production Container
```dockerfile
# Uses main branch
FROM failure-resolver:latest

RUN git clone -b main \
    https://github.com/bellboy-robotics/failure-resolver-database.git \
    /app/memory
```

### Testing Container
```dockerfile
# Uses develop branch
FROM failure-resolver:latest

RUN git clone -b develop \
    https://github.com/bellboy-robotics/failure-resolver-database.git \
    /app/memory
```

### Docker Compose
```yaml
services:
  failure-resolver-prod:
    environment:
      DB_BRANCH: main      # Production

  failure-resolver-test:
    environment:
      DB_BRANCH: develop   # Testing
```

---

## Best Practices

### ✓ DO

- ✓ Use `develop` for daily/frequent imports
- ✓ Test failures on `develop` before promoting
- ✓ Merge `develop` → `main` after validation
- ✓ Write clear commit messages with counts
- ✓ Review changes before major merges
- ✓ Keep main branch clean and stable

### ✗ DON'T

- ✗ Commit unvalidated data to `main`
- ✗ Push directly to `main` without testing
- ✗ Merge without reviewing differences
- ✗ Keep failures in `develop` indefinitely
- ✗ Mix test and production data

---

## Commit Message Convention

### For develop (Testing)
```
[TEST] <description>
  Example: "[TEST] Daily import: 10 robot failures from 2026-07-28"
  Example: "[TEST] Validation run: added solutions for 5 failures"
```

### For main (Production)
```
[PROD] <description>
  Example: "[PROD] Validated and promoted: 50 failures from July batch"
  Example: "[PROD] Production update: 3 new solution patterns"
```

### General
```
<count> <action>: <description>
  Example: "5 failures added: floor mapping issues detected"
  Example: "Validated 10 failures: all solutions tested successfully"
  Example: "Production merge: 50 failures from Q3 testing"
```

---

## Commands Reference

### Sync to Testing (develop)
```bash
python3 sync_database.py "Daily import: 10 new failures" develop
```

### Sync to Production (main)
```bash
python3 sync_database.py "Validated: 50 failures ready for production" main
```

### List branches and contents
```bash
# Via GitHub
https://github.com/bellboy-robotics/failure-resolver-database/branches

# Via Git
git clone https://github.com/bellboy-robotics/failure-resolver-database.git
git branch -a
```

### Compare branches
```bash
git diff main develop --stat
git log main..develop --oneline
```

### Merge develop to main
```bash
git checkout main
git merge develop
git push origin main
```

---

## CI/CD Integration

### GitHub Actions: Auto-sync develop
```yaml
name: Daily Test Sync

on:
  schedule:
    - cron: '0 1 * * *'  # 1 AM daily

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - run: python3 sync_database.py "Daily automated test sync" develop
```

### GitHub Actions: Production merge approval
```yaml
name: Production Merge

on:
  workflow_dispatch:  # Manual trigger only

jobs:
  promote:
    runs-on: ubuntu-latest
    steps:
      - run: git merge develop
      - run: python3 sync_database.py "Promoted to production" main
```

---

## Status

✓ Branching support added to `sync_database.py`
✓ Documentation created
⏳ Branches created on GitHub (manual step)
⏳ CI/CD configured (optional)

**Next Steps:**

1. Create GitHub repo: https://github.com/bellboy-robotics/failure-resolver-database

2. Create branches:
   ```bash
   git clone https://github.com/bellboy-robotics/failure-resolver-database.git
   git checkout -b develop
   git push -u origin develop
   ```

3. Test sync:
   ```bash
   python3 sync_database.py "Initial test failures" develop
   python3 sync_database.py "Initial production failures" main
   ```
