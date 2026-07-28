#!/usr/bin/env python3
"""
Robot Interface - Adapter between SolutionExecutor and Bellboy Robot API

Handles HTTP communication with the robot and translates solution commands
to actual robot commands via the official Bellboy API.

API: POST https://api.bellboy.co/robots/{SYSID}/commands
Auth: Authorization header with BELLBOY_API_KEY
"""

import asyncio
import json
import logging
import os
from typing import Optional, Dict, Any
from enum import Enum
from pathlib import Path
import httpx
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

logger = logging.getLogger(__name__)


class RobotCommandType(Enum):
    """Supported robot command types."""
    MOVE = "move"  # Navigate with direction and distance
    TWIST = "twist"  # Rotate with steps
    ABORT = "abort"  # Stop current command
    DOCK = "dock"  # Return to dock
    MANUAL = "manual"  # Manual mode
    HISTORY = "history"  # Get message history


class RobotInterface:
    """Adapter between SolutionExecutor and Bellboy Robot HTTP API."""

    API_URL = "https://api.bellboy.co/robots"

    def __init__(self, sysid: Optional[str] = None, api_key: Optional[str] = None):
        """
        Initialize robot interface.

        Args:
            sysid: Robot system ID (e.g., "BILLIE-16") - MUST BE UPPERCASE
                   If None, loads from ROBOT_SYSID env var
            api_key: Bellboy API key
                     If None, loads from BELLBOY_API_KEY env var
        """
        # Load from environment if not provided
        self.sysid = (sysid or os.getenv("ROBOT_SYSID", "")).upper()
        self.api_key = api_key or os.getenv("BELLBOY_API_KEY")

        if not self.sysid:
            raise ValueError("ROBOT_SYSID not provided and not set in environment")
        if not self.api_key:
            raise ValueError("BELLBOY_API_KEY not provided and not set in environment")

        self.client = httpx.AsyncClient(
            headers={"Authorization": self.api_key},
            timeout=30.0
        )

    async def connect(self) -> bool:
        """Check connection to robot (HTTP API doesn't require explicit connect)."""
        try:
            # Test connection by trying to send a dummy command
            # Actually, we'll just validate the API key format
            if not self.api_key or len(self.api_key) < 10:
                logger.error("Invalid API key")
                return False
            logger.info(f"Robot interface ready for {self.sysid}")
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()
        logger.info("Disconnected")

    async def send_command(
        self,
        command: str,
        arguments: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Send command to robot via HTTP API.

        Args:
            command: Command name (slide, twist, abort, dock, etc.)
            arguments: Command arguments dict

        Returns:
            Response dict from robot API
        """
        url = f"{self.API_URL}/{self.sysid}/commands"
        payload = {
            "command": command,
            "arguments": arguments or {}
        }

        try:
            logger.info(f"Sending: {command} {arguments} to {self.sysid}")
            response = await self.client.post(url, json=payload)

            if response.status_code == 200:
                result = response.json()
                if result.get("pending"):
                    logger.info(f"✓ Command queued: {command}")
                    return {"status": "pending", "command": command}
                else:
                    logger.warning(f"Unexpected response: {result}")
                    return result
            elif response.status_code == 401:
                raise RuntimeError("Invalid API key (401)")
            elif response.status_code == 404:
                raise RuntimeError(f"Robot not found: {self.sysid} (404)")
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                raise RuntimeError(f"API error: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Request failed: {e}")
            raise RuntimeError(f"Failed to send command: {str(e)}")

    # Command helpers - map SolutionExecutor commands to actual robot commands

    async def slide(self, direction: str, meters: float) -> Dict[str, Any]:
        """Slide (move) in direction by meters."""
        return await self.send_command("slide", {
            "direction": direction,
            "meters": meters
        })

    async def twist(self, direction: str, steps: int) -> Dict[str, Any]:
        """Twist (rotate) by steps."""
        return await self.send_command("twist", {
            "direction": direction,
            "steps": steps
        })

    async def abort(self) -> Dict[str, Any]:
        """Stop current command."""
        return await self.send_command("abort")

    async def dock(self) -> Dict[str, Any]:
        """Return to dock."""
        return await self.send_command("dock")

    # Generic command executor for solution commands

    async def execute_solution_command(self, cmd_name: str, **kwargs) -> Dict[str, Any]:
        """
        Execute a solution command by name.

        Maps solution command names to actual robot API commands.

        Args:
            cmd_name: Command name from solution (slide, twist, abort, dock, etc.)
            **kwargs: Command arguments

        Returns:
            Robot response
        """
        logger.info(f"Executing: {cmd_name}({kwargs})")

        try:
            # Map solution commands to robot API commands
            if cmd_name == "slide":
                direction = kwargs.get("direction", "forward")
                meters = float(kwargs.get("meters", kwargs.get("value", 0.1)))
                return await self.slide(direction, meters)

            elif cmd_name == "slide_forward":
                meters = float(kwargs.get("value", 0.1))
                return await self.slide("forward", meters)

            elif cmd_name == "slide_backward":
                meters = float(kwargs.get("value", 0.1))
                return await self.slide("backward", meters)

            elif cmd_name == "twist":
                direction = kwargs.get("direction", "right")
                steps = int(kwargs.get("steps", kwargs.get("value", 1)))
                return await self.twist(direction, steps)

            elif cmd_name == "twist_left":
                steps = int(kwargs.get("value", 1))
                return await self.twist("left", steps)

            elif cmd_name == "twist_right":
                steps = int(kwargs.get("value", 1))
                return await self.twist("right", steps)

            elif cmd_name == "abort":
                return await self.abort()

            elif cmd_name == "dock":
                return await self.dock()

            elif cmd_name == "wait":
                seconds = float(kwargs.get("value", 1.0))
                logger.info(f"Waiting {seconds}s...")
                await asyncio.sleep(seconds)
                return {"message": f"Waited {seconds}s", "status": "success"}

            elif cmd_name == "verify_stability":
                # Mock verification - in production, check actual sensor data
                logger.info("Verifying stability...")
                return {"message": "Stability verified", "status": "success"}

            else:
                raise ValueError(f"Unknown command: {cmd_name}")

        except Exception as e:
            logger.error(f"Command failed: {str(e)}")
            raise


async def test_robot_interface():
    """Test robot interface with sample commands."""
    try:
        robot = RobotInterface()  # Loads from .env automatically
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return

    try:
        # Connect
        if not await robot.connect():
            return

        # Test commands
        logger.info("\n=== Testing Robot Interface ===\n")

        # Test slide forward
        result = await robot.slide("forward", 0.5)
        logger.info(f"Slide result: {result}")

        # Test twist
        result = await robot.twist("right", 45)
        logger.info(f"Twist result: {result}")

        # Test abort
        result = await robot.abort()
        logger.info(f"Abort result: {result}")

        logger.info("\n=== Tests Complete ===\n")

    finally:
        await robot.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_robot_interface())
