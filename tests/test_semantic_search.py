#!/usr/bin/env python3
"""
Test: Semantic search for similar failures

Uses Qdrant to find failures similar to a given query.
"""

import json
import sys
from pathlib import Path

# Try to import required modules
try:
    from qdrant_client import QdrantClient
    from sentence_transformers import SentenceTransformer
    QDRANT_AVAILABLE = True
except ImportError:
    QDRANT_AVAILABLE = False
    print("Note: Qdrant/sentence-transformers not available locally")
    print("      Run this in Docker for full semantic search\n")


def test_semantic_search():
    """Test semantic search using embeddings."""

    print("\n" + "="*60)
    print("Semantic Failure Search Test")
    print("="*60 + "\n")

    if not QDRANT_AVAILABLE:
        print("✗ Required modules not installed locally")
        print("  Skipping semantic search test")
        return False

    # Load metadata
    index_file = Path("./memory/index.json")
    if not index_file.exists():
        print("✗ No memory index found. Run import_failures.py first")
        return False

    with open(index_file) as f:
        metadata = json.load(f)

    print(f"[1/4] Loaded {len(metadata)} failures from memory\n")

    # Initialize Qdrant client
    print("[2/4] Connecting to Qdrant...")
    try:
        client = QdrantClient(host="localhost", port=6333)
        print("  ✓ Connected to Qdrant\n")
    except Exception as e:
        print(f"  ✗ Cannot connect to Qdrant: {e}")
        print("  (Qdrant needs to be running)\n")
        return False

    # Initialize embedding model
    print("[3/4] Loading embedding model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print("  ✓ Model loaded\n")

    # Test semantic search
    print("[4/4] Searching for similar failures...")
    test_query = "Map is not the expected floor"

    print(f"  Query: \"{test_query}\"\n")

    try:
        # Generate embedding for query
        query_embedding = model.encode(test_query).tolist()

        # Search in Qdrant
        results = client.search(
            collection_name="failures",
            query_vector=query_embedding,
            limit=3
        )

        if results:
            print(f"  ✓ Found {len(results)} similar failures:\n")
            for i, hit in enumerate(results, 1):
                failure_id = hit.payload.get("failure_id", "unknown")
                error = hit.payload.get("error", "")[:70]
                score = hit.score

                print(f"  [{i}] {failure_id} (similarity: {score:.2f})")
                print(f"      Error: {error}...")

            return True
        else:
            print("  ✗ No similar failures found")
            return False

    except Exception as e:
        print(f"  ✗ Search failed: {e}")
        return False


if __name__ == "__main__":
    success = test_semantic_search()
    sys.exit(0 if success else 1)
