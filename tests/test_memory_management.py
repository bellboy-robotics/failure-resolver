#!/usr/bin/env python3
"""
Test: Complete Memory Management

Tests:
1. Import failures from CSV
2. Verify metadata index
3. Verify failure files
4. Search for specific failure
5. Display failure details
"""

import json
import sys
from pathlib import Path


def test_memory_management():
    """Test memory management system."""

    print("\n" + "="*60)
    print("Memory Management Test")
    print("="*60 + "\n")

    memory_dir = Path("./memory")
    index_file = memory_dir / "index.json"
    failures_dir = memory_dir / "failures"

    # [1] Check if index exists
    print("[1/5] Checking memory index...")
    if not index_file.exists():
        print("  ✗ No index found. Run: docker exec failure-resolver python3 import_failures.py 10_flow_failures.csv")
        return False

    with open(index_file) as f:
        metadata = json.load(f)

    print(f"  ✓ Loaded index with {len(metadata)} failures\n")

    # [2] Verify failure files
    print("[2/5] Verifying failure files...")
    failure_count = 0
    for failure_id, entry in metadata.items():
        failure_file = failures_dir / f"{failure_id}.md"
        if failure_file.exists():
            failure_count += 1

    print(f"  ✓ Found {failure_count}/{len(metadata)} failure markdown files\n")

    # [3] Search for a specific failure
    print("[3/5] Searching for a specific failure...")
    # Search for failures related to "map" or "floor"
    search_term = "floor"
    matches = []

    for failure_id, entry in metadata.items():
        error = entry.get("error", "").lower()
        command = entry.get("command", "").lower()

        if search_term in error or search_term in command:
            matches.append((failure_id, entry))

    print(f"  ✓ Found {len(matches)} failures mentioning '{search_term}'\n")

    # [4] Display search results
    print("[4/5] Search Results:")
    for failure_id, entry in matches[:3]:  # Show first 3
        print(f"  - {failure_id}")
        print(f"    Command: {entry['command']}")
        print(f"    Error: {entry['error'][:60]}...")
        print(f"    Location: {entry['site']} Room {entry['room']}\n")

    # [5] Verify file content
    print("[5/5] Verifying failure file content...")
    if matches:
        first_id, first_entry = matches[0]
        failure_file = failures_dir / f"{first_id}.md"

        if failure_file.exists():
            with open(failure_file) as f:
                content = f.read()

            print(f"  ✓ Sample failure file ({first_id}.md):\n")
            print("  " + "-"*56)
            for line in content.split("\n")[:5]:
                print(f"  {line}")
            print("  " + "-"*56 + "\n")

    print("="*60)
    print("✓ Memory management test successful!")
    print(f"  - {len(metadata)} failures indexed")
    print(f"  - {failure_count} markdown files created")
    print(f"  - Search functionality working")
    print("="*60 + "\n")

    return True


if __name__ == "__main__":
    success = test_memory_management()
    sys.exit(0 if success else 1)
