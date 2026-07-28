import os
import json
import logging
import asyncio
from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class SQSHandler:
    """Handle SQS messages for failures and solutions."""

    def __init__(self):
        self.sqs_client = boto3.client(
            "sqs",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.failures_queue_url = os.getenv("FAILURES_QUEUE_URL")
        self.solutions_queue_url = os.getenv("SOLUTIONS_QUEUE_URL")
        self.poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", 5))

    async def poll_failures(self, callback):
        """Poll failures queue and process messages."""
        if not self.failures_queue_url:
            logger.warning("FAILURES_QUEUE_URL not set, skipping failure polling")
            return

        while True:
            try:
                response = self.sqs_client.receive_message(
                    QueueUrl=self.failures_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=10,
                )

                if "Messages" in response:
                    for message in response["Messages"]:
                        await self._process_failure(message, callback)
                else:
                    await asyncio.sleep(self.poll_interval)

            except ClientError as e:
                logger.error(f"SQS error polling failures: {str(e)}")
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Unexpected error polling failures: {str(e)}")
                await asyncio.sleep(self.poll_interval)

    async def poll_solutions(self, callback):
        """Poll solutions queue and process messages."""
        if not self.solutions_queue_url:
            logger.warning("SOLUTIONS_QUEUE_URL not set, skipping solutions polling")
            return

        while True:
            try:
                response = self.sqs_client.receive_message(
                    QueueUrl=self.solutions_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=10,
                )

                if "Messages" in response:
                    for message in response["Messages"]:
                        await self._process_solution(message, callback)
                else:
                    await asyncio.sleep(self.poll_interval)

            except ClientError as e:
                logger.error(f"SQS error polling solutions: {str(e)}")
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Unexpected error polling solutions: {str(e)}")
                await asyncio.sleep(self.poll_interval)

    async def _process_failure(self, message: Dict, callback):
        """Process a failure message from SQS."""
        try:
            body = json.loads(message["Body"])
            receipt_handle = message["ReceiptHandle"]

            logger.info(f"Processing failure: {body.get('robot_id')}")

            # Call the callback to process the failure
            result = await callback(body)

            # Delete message only if processing succeeded
            if result:
                self.sqs_client.delete_message(
                    QueueUrl=self.failures_queue_url, ReceiptHandle=receipt_handle
                )
                logger.info(f"Deleted failure message {body.get('robot_id')}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse failure message: {str(e)}")
            self._delete_message(message, self.failures_queue_url)
        except Exception as e:
            logger.error(f"Error processing failure: {str(e)}")
            # Don't delete on error - let it retry

    async def _process_solution(self, message: Dict, callback):
        """Process a solution message from SQS."""
        try:
            body = json.loads(message["Body"])
            receipt_handle = message["ReceiptHandle"]

            logger.info(f"Processing solution for failure: {body.get('failure_id')}")

            # Call the callback to process the solution
            result = await callback(body)

            # Delete message only if processing succeeded
            if result:
                self.sqs_client.delete_message(
                    QueueUrl=self.solutions_queue_url, ReceiptHandle=receipt_handle
                )
                logger.info(f"Deleted solution message {body.get('failure_id')}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse solution message: {str(e)}")
            self._delete_message(message, self.solutions_queue_url)
        except Exception as e:
            logger.error(f"Error processing solution: {str(e)}")
            # Don't delete on error - let it retry

    def _delete_message(self, message: Dict, queue_url: str):
        """Delete a message from the queue."""
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
        except Exception as e:
            logger.error(f"Failed to delete message: {str(e)}")

    def send_result(self, queue_url: str, result: Dict):
        """Send a result message to a queue (for responses)."""
        try:
            self.sqs_client.send_message(
                QueueUrl=queue_url, MessageBody=json.dumps(result)
            )
            logger.info(f"Sent result to queue: {result}")
        except Exception as e:
            logger.error(f"Failed to send result: {str(e)}")
