#!/usr/bin/env python3
"""
Test script: Use RobotInterface to send commands

This tests the actual RobotInterface class that will be used by SolutionExecutor.
Commands come as a list and are executed one by one.
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from robot_interface import RobotInterface

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_robot_interface():
    """Test RobotInterface with a list of commands."""

    # Get config from environment
    api_key = os.getenv("BELLBOY_API_KEY")
    sysid = os.getenv("ROBOT_SYSID", "BILLIE-16")

    if not api_key:
        logger.error("BELLBOY_API_KEY environment variable not set")
        return False

    logger.info(f"\n{'='*60}")
    logger.info("RobotInterface Integration Test")
    logger.info(f"{'='*60}\n")

    # Initialize robot interface
    robot = RobotInterface(sysid, api_key)

    # Connect
    logger.info("[1/4] Connecting to robot...")
    if not await robot.connect():
        logger.error("Failed to connect")
        return False

    try:
        # Commands to execute (as a list, one by one)
        commands = [
            {
                "name": "slide",
                "kwargs": {
                    "direction": "backward",
                    "meters": 0.1,
                    "stuck_attempts": 3,
                    "stuck_detection_precision_cm": 0.2,
                    "timeout_seconds": 60,
                    "wait": True
                }
            },
            {
                "name": "wait",
                "kwargs": {"value": 1.0}
            },
            {
                "name": "verify_stability",
                "kwargs": {}
            }
        ]

        logger.info(f"[2/4] Executing {len(commands)} commands from list...\n")

        results = []
        for i, cmd in enumerate(commands, 1):
            cmd_name = cmd["name"]
            cmd_kwargs = cmd["kwargs"]

            logger.info(f"[{i}/{len(commands)}] Executing: {cmd_name}({cmd_kwargs})")

            try:
                # Execute via RobotInterface
                result = await robot.execute_solution_command(cmd_name, **cmd_kwargs)
                logger.info(f"  ✓ Result: {json.dumps(result, indent=4)}\n")
                results.append({
                    "command": cmd_name,
                    "status": "success",
                    "result": result
                })

            except Exception as e:
                logger.error(f"  ✗ Error: {e}\n")
                results.append({
                    "command": cmd_name,
                    "status": "failed",
                    "error": str(e)
                })

        # Summary
        logger.info(f"[3/4] Execution Summary")
        logger.info(f"{'='*60}")
        successful = sum(1 for r in results if r["status"] == "success")
        logger.info(f"Total: {len(results)} commands")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {len(results) - successful}")
        logger.info(f"{'='*60}\n")

        return successful == len(results)

    finally:
        logger.info("[4/4] Disconnecting...")
        await robot.disconnect()


if __name__ == "__main__":
    success = asyncio.run(test_robot_interface())
    sys.exit(0 if success else 1)
