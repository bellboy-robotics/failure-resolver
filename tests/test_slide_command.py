#!/usr/bin/env python3
"""
Test script: Send a slide command to the robot
"""

import asyncio
import json
import logging
import os
import httpx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_slide():
    """Send slide command to robot."""

    # Get config from environment
    api_key = os.getenv("BELLBOY_API_KEY")
    sysid = os.getenv("ROBOT_SYSID", "BILLIE-10")

    if not api_key:
        logger.error("BELLBOY_API_KEY environment variable not set")
        return False

    url = f"https://api.bellboy.co/robots/{sysid}/commands"

    payload = {
        "command": "slide",
        "arguments": {
            "direction": "backward",
            "meters": 0.1,
            "stuck_attempts": 3,
            "stuck_detection_precision_cm": 0.2,
            "timeout_seconds": 60,
            "wait": True
        }
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"Sending command to: {url}")
    logger.info(f"Payload: {json.dumps(payload, indent=2)}")
    logger.info(f"{'='*60}\n")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                json=payload,
                headers={"Authorization": api_key}
            )

            logger.info(f"Status Code: {response.status_code}")
            logger.info(f"Response: {json.dumps(response.json(), indent=2)}")

            if response.status_code == 200:
                result = response.json()
                if result.get("pending"):
                    logger.info("\n✓ Command queued successfully")
                    return True
                else:
                    logger.info(f"\n✓ Response: {result}")
                    return True
            else:
                logger.error(f"\n✗ API error {response.status_code}")
                return False

    except Exception as e:
        logger.error(f"✗ Request failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_slide())
    exit(0 if success else 1)
