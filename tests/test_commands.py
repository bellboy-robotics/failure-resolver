#!/usr/bin/env python3
"""
Test: Execute commands via RobotInterface

Commands come as a list and are executed one by one.
RobotInterface loads API key and robot ID from .env automatically.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from robot_interface import RobotInterface

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def execute_commands():
    """Execute a list of commands via RobotInterface."""

    logger.info(f"\n{'='*60}")
    logger.info("Command Execution Test")
    logger.info(f"{'='*60}\n")

    # Initialize robot interface (loads from .env)
    try:
        robot = RobotInterface()
        logger.info(f"Initialized for robot: {robot.sysid}")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return False

    # Connect
    logger.info("Connecting to robot...")
    if not await robot.connect():
        logger.error("Failed to connect")
        return False

    try:
        # Commands to execute (as a list, one by one)
        # Same slide command as test_slide_command.py
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
            }
        ]

        logger.info(f"Executing {len(commands)} commands...\n")

        results = []
        for i, cmd in enumerate(commands, 1):
            cmd_name = cmd["name"]
            cmd_kwargs = cmd["kwargs"]

            logger.info(f"[{i}/{len(commands)}] {cmd_name}({cmd_kwargs})")

            try:
                result = await robot.execute_solution_command(cmd_name, **cmd_kwargs)
                logger.info(f"  ✓ Success: {result}\n")
                results.append({
                    "command": cmd_name,
                    "status": "success",
                    "result": result
                })

            except Exception as e:
                logger.error(f"  ✗ Failed: {e}\n")
                results.append({
                    "command": cmd_name,
                    "status": "failed",
                    "error": str(e)
                })

        # Summary
        logger.info(f"{'='*60}")
        successful = sum(1 for r in results if r["status"] == "success")
        logger.info(f"Total: {len(results)} commands")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {len(results) - successful}")
        logger.info(f"{'='*60}\n")

        return successful == len(results)

    finally:
        logger.info("Disconnecting...")
        await robot.disconnect()


async def main():
    success = await execute_commands()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
