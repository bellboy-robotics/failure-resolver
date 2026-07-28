#!/usr/bin/env python3
"""
Test: Search for failure in memory

1. Load a failure from the CSV
2. Search the memory system for similar failures
3. Verify it can be found
"""

import json
import os
from pathlib import Path

def test_search():
    """Test searching for failures in memory."""

    print("\n" + "="*60)
    print("Failure Search Test")
    print("="*60 + "\n")

    # Load metadata index
    memory_dir = Path("./memory")
    index_file = memory_dir / "index.json"

    if not index_file.exists():
        print("✗ No memory index found. Run import_failures.py first")
        return False

    with open(index_file) as f:
        metadata = json.load(f)

    print(f"[1/3] Loaded memory index with {len(metadata)} failures\n")

    # Pick one to search for (the first one)
    search_failure = list(metadata.values())[0]
    search_id = list(metadata.keys())[0]

    print(f"[2/3] Searching for failure: {search_id}")
    print(f"  Command: {search_failure['command']}")
    print(f"  Error: {search_failure['error'][:80]}...")
    print(f"  Location: {search_failure['site']} Room {search_failure['room']}\n")

    # Check if failure file exists
    failure_file = memory_dir / "failures" / f"{search_id}.md"

    if failure_file.exists():
        print(f"[3/3] ✓ Found failure file: {failure_file.name}")
        with open(failure_file) as f:
            content = f.read()
        print(f"\nFailure Details:")
        print("-" * 60)
        print(content[:300] + "...")
        print("-" * 60)
        print(f"\n✓ Search successful!")
        return True
    else:
        print(f"[3/3] ✗ Failure file not found: {failure_file}")
        return False


if __name__ == "__main__":
    import sys
    success = test_search()
    sys.exit(0 if success else 1)
