#!/usr/bin/env python3
"""
Test: Search Success and Failure Cases

Tests:
1. Search for existing failure (expected to succeed)
2. Search for random text (expected to fail)
"""

import json
import sys
from pathlib import Path


def search_failures(search_term):
    """Search for failures by keyword."""
    memory_dir = Path("./memory")
    index_file = memory_dir / "index.json"
    failures_dir = memory_dir / "failures"

    with open(index_file) as f:
        metadata = json.load(f)

    matches = []

    for failure_id, entry in metadata.items():
        error = entry.get("error", "").lower()
        command = entry.get("command", "").lower()
        site = entry.get("site", "").lower()
        room = entry.get("room", "").lower()

        if (search_term.lower() in error or
            search_term.lower() in command or
            search_term.lower() in site or
            search_term.lower() in room):
            matches.append((failure_id, entry))

    return matches


def test_search():
    """Test search success and failure cases."""

    print("\n" + "="*60)
    print("Search Success/Failure Test")
    print("="*60 + "\n")

    # Load failures to pick one
    memory_dir = Path("./memory")
    index_file = memory_dir / "index.json"

    if not index_file.exists():
        print("✗ No memory index found. Run import_failures.py first")
        return False

    with open(index_file) as f:
        metadata = json.load(f)

    all_failures = list(metadata.values())

    # Test 1: Search for existing failure (should succeed)
    print("[TEST 1] Search for existing failure")
    print("-" * 60)

    # Pick a real error from the first failure
    test_failure = all_failures[0]
    search_query = "Current map is expected"  # Part of actual error

    print(f"Query: \"{search_query}\"")
    print(f"Expected: SUCCESS (should find 2+ failures)\n")

    results = search_failures(search_query)

    if results:
        print(f"✓ SUCCESS: Found {len(results)} matching failures\n")
        for failure_id, entry in results[:2]:
            print(f"  - {failure_id}")
            print(f"    Error: {entry['error'][:60]}...")
        print()
        test1_pass = True
    else:
        print("✗ FAILED: No results found\n")
        test1_pass = False

    # Test 2: Search for random text (should fail)
    print("[TEST 2] Search for random text")
    print("-" * 60)

    random_query = "xyzqwerty123notreal"
    print(f"Query: \"{random_query}\"")
    print(f"Expected: FAILURE (should find 0 failures)\n")

    results = search_failures(random_query)

    if not results:
        print(f"✓ SUCCESS: No results found (as expected)\n")
        test2_pass = True
    else:
        print(f"✗ FAILED: Found {len(results)} results (expected 0)\n")
        for failure_id, entry in results:
            print(f"  - {failure_id}: {entry['error'][:60]}...")
        test2_pass = False

    # Summary
    print("="*60)
    print("Test Results:")
    print(f"  Test 1 (search existing): {'PASS ✓' if test1_pass else 'FAIL ✗'}")
    print(f"  Test 2 (search random):   {'PASS ✓' if test2_pass else 'FAIL ✗'}")
    print("="*60 + "\n")

    return test1_pass and test2_pass


if __name__ == "__main__":
    success = test_search()
    sys.exit(0 if success else 1)
