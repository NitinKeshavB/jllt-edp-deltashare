"""
Workflow Queue Module

Azure Storage Queue integration for async share pack processing.
"""

from dbrx_api.workflow.queue.queue_client import SharePackQueueClient
from dbrx_api.workflow.queue.queue_consumer import start_queue_consumer

__all__ = [
    "SharePackQueueClient",
    "start_queue_consumer",
]
