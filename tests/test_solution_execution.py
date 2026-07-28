#!/usr/bin/env python3
"""
Test Script: Solution Execution Flow

Flow:
1. Get robot token from environment
2. Connect to robot via WebSocket
3. Execute solution commands
4. Report results

Usage:
    export ROBOT_SYSID=billie-10
    export ROBOT_TOKEN=<your_token>
    python test_solution_execution.py
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from robot_interface import RobotInterface
from solution_executor import SolutionExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_flow():
    """Test the complete failure resolution flow."""

    # Get configuration from environment
    sysid = os.getenv("ROBOT_SYSID", "BILLIE-10")
    api_key = os.getenv("BELLBOY_API_KEY")

    if not api_key:
        logger.error("BELLBOY_API_KEY environment variable not set")
        logger.info("Get API key from your Bellboy account settings")
        return False

    logger.info(f"\n{'='*60}")
    logger.info("FAILURE RESOLVER - Solution Execution Test")
    logger.info(f"{'='*60}\n")

    # Initialize robot interface
    logger.info(f"[1/4] Initializing robot interface for {sysid}...")
    robot = RobotInterface(sysid, api_key)

    # Connect to robot
    logger.info("[2/4] Connecting to robot...")
    if not await robot.connect():
        logger.error("Failed to connect to robot")
        return False

    try:
        # Create solution executor
        logger.info("[3/4] Creating solution executor...")
        executor = SolutionExecutor(robot_interface=robot.execute_solution_command)

        # Test solution 1: Simple slide and verify
        logger.info("\n[4/4] Executing test solution 1: Slide forward and verify...\n")

        solution_1 = [
            "slide_forward(0.5)",
            "verify_stability()"
        ]

        logger.info(f"Solution: {json.dumps(solution_1, indent=2)}\n")

        # Validate first (dry-run)
        logger.info("→ Validating solution (dry-run)...")
        result = await executor.execute_solution(solution_1, dry_run=True)
        logger.info(f"Validation result: {result['status']}\n")

        if result['status'] != 'dry_run_ok':
            logger.error("Validation failed")
            return False

        # Execute for real
        logger.info("→ Executing solution on robot...")
        result = await executor.execute_solution(solution_1, dry_run=False)

        # Report results
        logger.info("\n" + "="*60)
        logger.info("EXECUTION RESULTS")
        logger.info("="*60)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Commands executed: {result['total']}")
        logger.info(f"Successful: {result['successful']}")

        for cmd_result in result['commands']:
            status_icon = "✓" if cmd_result['status'] == 'success' else "✗"
            logger.info(f"\n{status_icon} {cmd_result['command']}")
            logger.info(f"  Status: {cmd_result['status']}")
            if cmd_result['status'] == 'success':
                logger.info(f"  Response: {cmd_result.get('result', {}).get('message', 'OK')}")
            else:
                logger.info(f"  Error: {cmd_result.get('error', 'Unknown error')}")

        logger.info("\n" + "="*60)

        # Test solution 2: More complex commands
        logger.info("\n[Optional] Testing solution 2: Movement commands...\n")

        solution_2 = [
            "slide_forward(1.5)",
            "twist_right(45)",
            "wait(2)",
            "verify_stability()"
        ]

        logger.info(f"Solution: {json.dumps(solution_2, indent=2)}\n")

        result = await executor.execute_solution(solution_2, dry_run=False)
        logger.info(f"Result: {result['successful']}/{result['total']} commands succeeded")

        return True

    finally:
        # Disconnect
        logger.info("\nDisconnecting from robot...")
        await robot.disconnect()
        logger.info("✓ Test complete\n")


async def main():
    """Main entry point."""
    try:
        success = await test_flow()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nTest interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
