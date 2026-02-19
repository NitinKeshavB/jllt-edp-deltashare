"""
Azure Storage Queue Client for Share Pack Processing

Wraps Azure Storage Queue for enqueuing and dequeuing share pack provisioning tasks.
"""

import json
from typing import Optional

from loguru import logger

try:
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.queue import QueueClient

    AZURE_QUEUE_AVAILABLE = True
except ImportError:
    logger.warning("azure-storage-queue not installed - workflow queue will not work")
    AZURE_QUEUE_AVAILABLE = False
    QueueClient = None
    ResourceExistsError = None


class SharePackQueueClient:
    """
    Share pack queue client for async provisioning.

    Wraps Azure Storage Queue for enqueuing share packs and providing message operations.
    """

    def __init__(self, connection_string: str, queue_name: str):
        """
        Initialize queue client.

        Args:
            connection_string: Azure Storage Queue connection string
            queue_name: Queue name (e.g., "sharepack-processing")
        """
        if not AZURE_QUEUE_AVAILABLE:
            raise RuntimeError("azure-storage-queue not installed")

        self.connection_string = connection_string
        self.queue_name = queue_name
        self.client: Optional[QueueClient] = None

        # Create queue client and ensure queue exists
        self._initialize_queue()

    def _initialize_queue(self):
        """Initialize queue client and create queue if doesn't exist."""
        self.client = QueueClient.from_connection_string(self.connection_string, self.queue_name)

        # Create queue (idempotent - no error if already exists)
        try:
            self.client.create_queue()
            logger.info(f"Queue '{self.queue_name}' ready")
        except ResourceExistsError:
            # Queue already exists - this is expected and OK
            logger.debug("Queue exists")
        except Exception as e:
            # Other error during queue creation
            logger.debug(f"Queue creation message: {e}")

    def enqueue_sharepack(self, share_pack_id: str, share_pack_name: str) -> None:
        """
        Enqueue a share pack for provisioning.

        Args:
            share_pack_id: Share pack UUID (as string)
            share_pack_name: Share pack display name

        Raises:
            Exception: If queue operation fails
        """
        message = {
            "share_pack_id": share_pack_id,
            "share_pack_name": share_pack_name,
        }

        try:
            self.client.send_message(json.dumps(message))
            logger.info(f"Enqueued share pack for provisioning: {share_pack_id}")
        except Exception as e:
            logger.error(f"Failed to enqueue share pack {share_pack_id}: {e}")
            raise

    def receive_messages(
        self,
        max_messages: int = 1,
        visibility_timeout: int = 300,
    ):
        """
        Receive messages from queue.

        Messages become invisible for the visibility_timeout duration.

        Args:
            max_messages: Maximum number of messages to receive (default: 1)
            visibility_timeout: Seconds message stays invisible (default: 300 = 5 minutes)

        Returns:
            List of message objects

        Raises:
            Exception: If queue operation fails
        """
        try:
            messages = self.client.receive_messages(max_messages=max_messages, visibility_timeout=visibility_timeout)
            return list(messages)  # Convert generator to list
        except Exception as e:
            logger.error(f"Failed to receive messages from queue: {e}")
            raise

    def delete_message(self, message) -> None:
        """
        Delete (acknowledge) a message after successful processing.

        Args:
            message: Message object from receive_messages()

        Raises:
            Exception: If delete operation fails
        """
        try:
            self.client.delete_message(message)
            logger.debug(f"Deleted message from queue")
        except Exception as e:
            logger.error(f"Failed to delete message: {e}")
            raise

    def get_queue_length(self) -> int:
        """
        Get approximate number of messages in queue.

        Returns:
            Approximate message count
        """
        try:
            properties = self.client.get_queue_properties()
            return properties.approximate_message_count
        except Exception as e:
            logger.error(f"Failed to get queue length: {e}")
            return 0
