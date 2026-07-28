# Database Backup Strategy

## Architecture

Two separate GitHub repositories:

```
bellboy-robotics/
├── failure-resolver (code repo)
│   ├── *.py (source code)
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── sync_database.py (backup tool)
│
└── failure-resolver-database (data repo)
    ├── index.json (metadata index)
    ├── failures/
    │   ├── failure_d02819d9.md
    │   ├── failure_ee891806.md
    │   └── ... (all failure markdown files)
    └── README.md
```

**Why separate?**
- ✓ Code changes don't pollute data repo
- ✓ Data can be updated independently
- ✓ Easy to clone just the database for restore
- ✓ CI/CD can work on each independently

---

## Workflow

### After Importing New Failures

```bash
# Step 1: Import failures into local memory
docker exec failure-resolver python3 import_failures.py new_failures.csv

# Step 2: Backup to GitHub
python3 sync_database.py "Added 10 new failures from daily import"
```

### What Gets Backed Up

```
memory/
├── index.json ✓ BACKED UP
│   └── Metadata: failure_id, command, error, location, etc.
│
└── failures/ ✓ BACKED UP
    ├── failure_d02819d9.md (markdown format)
    ├── failure_ee891806.md
    └── ... (human-readable, git-trackable)
```

### Not Backed Up

- `memory/metadata.db` (SQLite - can be regenerated)
- `memory/logs/` (logs)
- Docker volumes (Qdrant) (can be regenerated from markdown + embeddings)

---

## Quick Start

### Setup (One-time)

```bash
# Clone failure-resolver repo
git clone https://github.com/bellboy-robotics/failure-resolver.git
cd failure-resolver

# Create GitHub token with repo access
# https://github.com/settings/tokens/new
# Scopes: repo (full), workflow
```

### Import & Backup

```bash
# 1. Run import
docker exec failure-resolver python3 import_failures.py data.csv

# 2. Sync to GitHub
python3 sync_database.py "Added failures from 2026-07-28 run"

# Done! Check: https://github.com/bellboy-robotics/failure-resolver-database
```

---

## Commands

### Python Script (Recommended)
```bash
python3 sync_database.py "Commit message here"
```

**Output:**
```
============================================================
Database Sync to GitHub
============================================================

[1/5] Setting up database repository...
  📦 Cloning ...
  ✓ Cloned

[2/5] Preparing data directories...
  ✓ Ready

[3/5] Copying data files...
  ✓ Copied index.json
  ✓ Copied 10 failure files

[4/5] Checking for changes...
  📊 10 changes detected

[5/5] Committing and pushing...
  ✍️  Committed: Added failures from 2026-07-28 run
  📤 Pushed to GitHub

============================================================
✅ Database synced to GitHub!
  Repo: https://github.com/bellboy-robotics/failure-resolver-database
============================================================
```

### Bash Script Alternative
```bash
chmod +x backup_database.sh
./backup_database.sh "Added 10 failures"
```

---

## Restore from GitHub

### Full Restore

```bash
# Clone the database repo
git clone https://github.com/bellboy-robotics/failure-resolver-database.git
cp failure-resolver-database/index.json failure-resolver/memory/
cp failure-resolver-database/failures/* failure-resolver/memory/failures/

# Regenerate Qdrant vectors from markdown
docker exec failure-resolver python3 import_failures.py --restore-from-disk
```

### Single Failure Restore

```bash
# Get specific failure from GitHub
curl https://raw.githubusercontent.com/bellboy-robotics/failure-resolver-database/main/failures/failure_d02819d9.md \
  > memory/failures/failure_d02819d9.md
```

---

## GitHub Actions (Optional)

Automatic daily backup:

```yaml
# .github/workflows/backup-database.yml
name: Backup Database

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM daily
  workflow_dispatch:

jobs:
  backup:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run backup
        run: python3 sync_database.py "Daily automated backup"
      - name: Push to database repo
        uses: actions/github-script@v6
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
```

---

## Access Control

**GitHub Setup:**

1. Create `failure-resolver-database` repo (public or private)
2. Grant write access to failure-resolver team
3. Configure GitHub token in CI/CD (if automating)

**File Permissions:**

- `index.json` - Everyone can read
- `failures/*.md` - Everyone can read
- Write access - Only failure-resolver service

---

## Backup Frequency

**Recommended:**
- **Immediate:** After each CSV import
- **Automatic:** Daily via GitHub Actions
- **Manual:** Anytime before major operations

**Example Schedule:**
```
Robot detects failures
    ↓
Avidor sends to failure-resolver
    ↓
import_failures.py processes + stores locally
    ↓
sync_database.py commits to GitHub ← YOU ARE HERE
    ↓
GitHub has latest backup
    ↓
(monthly) Archive old data (optional)
```

---

## Data Safety

**Local Protection:**
- ✓ Failures persist in `memory/failures/` (disk)
- ✓ Index persists in `memory/index.json` (disk)
- ✓ Qdrant snapshots to disk

**Cloud Protection:**
- ✓ GitHub keeps full history
- ✓ Markdown is human-readable
- ✓ Can restore any version from git history

**Disaster Recovery:**
- ✓ Lose local data? Clone from GitHub
- ✓ Lose GitHub? Rebuild from local backup
- ✓ Lose both? Use most recent Qdrant snapshot

---

## Status

✓ `sync_database.py` - Ready
✓ `backup_database.sh` - Ready
✓ Architecture defined
⏳ `failure-resolver-database` repo - Needs creation on GitHub
⏳ GitHub token - Needs configuration

**Next Steps:**
1. Create repo: https://github.com/new (in bellboy-robotics org)
   - Name: `failure-resolver-database`
   - Description: "Failure database for failure-resolver service"
   - Private/Public: Your choice
   
2. Test sync:
   ```bash
   python3 sync_database.py "Initial commit with 10 failures"
   ```

3. Verify: https://github.com/bellboy-robotics/failure-resolver-database
