# Development Workflow - Test/Reset Cycle

## Philosophy

**During Development:** Use test data and reset frequently
**After Validation:** Switch to real data and commit to production branch

```
[Development Phase]           [Production Phase]
Reset frequently              Never reset
Test with dummy data          Real robot data
develop branch               main branch
```

---

## Quick Start: Test Cycle

### 1. Reset to Fresh State
```bash
python3 reset_database.py --force
```

**What it does:**
- ✓ Clears all failure markdown files
- ✓ Deletes index.json
- ✓ Resets Qdrant vectors
- ✓ Keeps code/config intact

### 2. Import Test Data
```bash
docker exec failure-resolver python3 import_failures.py 10_flow_failures.csv
```

### 3. Run Tests
```bash
python3 test_memory_management.py
python3 test_search_success_fail.py
python3 test_commands.py
```

### 4. Verify Results
```bash
# Check what was indexed
ls -la memory/failures/
python3 -c "import json; print(json.load(open('memory/index.json')))"
```

### 5. Backup Test Data (Optional)
```bash
# Save test data to develop branch (for reproducibility)
python3 sync_database.py "Test run #5: validated search feature" develop
```

### 6. Reset and Repeat
```bash
python3 reset_database.py --force
# Back to step 2...
```

---

## Full Development Cycle

```
Day 1: Feature Testing
┌─────────────────────────────────┐
│ 1. python3 reset_database.py    │
│ 2. Import test CSV              │
│ 3. Run feature tests            │
│ 4. Fix bugs                     │
│ 5. Repeat steps 1-4             │
│ 6. Sync to develop branch       │
└─────────────────────────────────┘
         ↓ (feature works)
         
Day 2-5: Integration Testing
┌─────────────────────────────────┐
│ 1. Keep current test data       │
│ 2. Test with real robot         │
│ 3. Verify solutions execute     │
│ 4. Check error handling         │
│ 5. Load test on develop branch  │
└─────────────────────────────────┘
         ↓ (confident feature is good)
         
Day N: Production Deployment
┌─────────────────────────────────┐
│ 1. Merge develop → main branch  │
│ 2. Deploy to production         │
│ 3. Start collecting real data   │
│ 4. NEVER reset main branch      │
└─────────────────────────────────┘
```

---

## Command Reference

### Reset Everything
```bash
# Ask for confirmation
python3 reset_database.py

# Force reset (no questions)
python3 reset_database.py --force
```

### Import Test Data
```bash
# From CSV file
docker exec failure-resolver python3 import_failures.py data.csv

# Check what was imported
ls memory/failures/ | wc -l
python3 test_memory_management.py
```

### Run Tests
```bash
# All tests
python3 test_memory_management.py
python3 test_search_success_fail.py
python3 test_commands.py

# Docker version
docker exec failure-resolver python3 test_memory_management.py
```

### Backup Test Results
```bash
# To develop (testing branch - OK to reset later)
python3 sync_database.py "Test iteration #3: search feature working" develop

# To main (production branch - DO NOT RESET)
python3 sync_database.py "Production ready: validated on real robots" main
```

### Clear Just the Failures (Keep Index)
```bash
rm -rf memory/failures/*.md
mkdir -p memory/failures
```

### Keep Index, Clear Everything Else
```bash
rm -rf memory/failures/*.md
rm memory/metadata.db
```

---

## Test Data Files

### Sample CSV for Testing
Create `test_data.csv`:
```csv
activity_id,site,room,floor,flow,command,activity_error,activity_start_time,activity_end_time
abc123,test-site,100,1,"Test Flow",navigate_poi,"Map not found",2026-07-28T10:00:00Z,2026-07-28T10:01:00Z
def456,test-site,200,2,"Test Flow",slide,"Slide failed",2026-07-28T10:02:00Z,2026-07-28T10:03:00Z
```

### Use Provided Sample
```bash
# Use the included test file
docker exec failure-resolver python3 import_failures.py 10_flow_failures.csv
```

---

## Workflow Examples

### Example 1: Quick Feature Test (10 minutes)
```bash
# Start
python3 reset_database.py --force

# Import test data
docker exec failure-resolver python3 import_failures.py 10_flow_failures.csv

# Test new feature
python3 test_search_success_fail.py

# Done - delete data
python3 reset_database.py --force
```

### Example 2: Full Integration Test (1 hour)
```bash
# Start
python3 reset_database.py --force

# Prepare test data
docker exec failure-resolver python3 import_failures.py large_test_set.csv

# Run all tests
python3 test_memory_management.py
python3 test_search_success_fail.py
python3 test_commands.py

# Test with real robot
docker exec failure-resolver python3 test_solution_execution.py

# Save results
python3 sync_database.py "Integration test #5: all systems working" develop

# Reset for next test
python3 reset_database.py --force
```

### Example 3: Staging to Production (one-time)
```bash
# Run final validation
python3 reset_database.py --force
docker exec failure-resolver python3 import_failures.py real_production_data.csv
python3 test_memory_management.py
docker exec failure-resolver python3 test_solution_execution.py

# Backup to production branch
python3 sync_database.py "PRODUCTION: Real failure data from Aug 2026" main

# DO NOT RESET AFTER THIS - main branch is live!
# From now on: only add data, never reset
```

---

## Important Rules

### ✓ Safe to Reset
- `develop` branch - for testing
- Local test data - always
- Test failures - fine
- Test index - fine

### ✗ DO NOT RESET
- `main` branch - that's production data
- Real failure data - use version control instead
- Production Qdrant - backup first

### When Switching to Real Data
```bash
# Before switching
python3 sync_database.py "FINAL TEST: 100 real failures validated" develop
git merge develop main
python3 sync_database.py "PRODUCTION: Real data deployment" main

# After switching: NEVER RESET
# Only add new real data via:
python3 sync_database.py "Production: 5 new failures from Aug 15" main
```

---

## Troubleshooting

### Database didn't reset fully
```bash
# Manual reset
docker-compose down
docker volume rm failure-resolver_qdrant_storage
rm -rf memory/failures/*
rm memory/index.json
docker-compose up -d
```

### Index corrupted
```bash
# Regenerate from markdown files
# (Coming in next update)
python3 regenerate_index.py
```

### Qdrant won't start after reset
```bash
docker-compose logs failure-resolver-qdrant
docker-compose restart failure-resolver-qdrant
```

---

## Summary

**Development:** Reset constantly, test with dummy data, use `develop` branch
**Production:** Never reset, real data, use `main` branch

When ready to go live:
```bash
# Final validation
python3 reset_database.py --force
docker exec failure-resolver python3 import_failures.py REAL_DATA.csv
python3 test_memory_management.py

# Promote to production
python3 sync_database.py "PRODUCTION LAUNCH: Real failure database" main

# Done! Switch robots to main branch
# Data is now live - no more resets
```
