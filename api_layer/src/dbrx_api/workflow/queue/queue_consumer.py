"""
Queue Consumer for Share Pack Processing

Background task that polls the queue and triggers provisioning.
"""

import asyncio
import json

from loguru import logger


async def start_queue_consumer(queue_client, db_pool):
    """
    Start background queue consumer for share pack provisioning.

    This function runs as an async background task in the web app.
    It polls the Azure Storage Queue and processes share packs.

    Args:
        queue_client: SharePackQueueClient instance
        db_pool: DomainDBPool instance

    Note:
        For MVP, this is a simplified implementation.
        Production version would include error handling, retry logic, etc.
    """
    logger.info("Share pack queue consumer started")

    # For MVP, we'll use a simple polling loop
    # In production, this would be more sophisticated with proper error handling
    while True:
        try:
            # Poll for messages
            messages = queue_client.receive_messages(max_messages=1, visibility_timeout=600)  # 10 minutes to process

            for msg in messages:
                try:
                    body = json.loads(msg.content)
                    share_pack_id = body["share_pack_id"]
                    share_pack_name = body.get("share_pack_name", "unknown")

                    logger.info(f"Processing share pack from queue: {share_pack_id} ({share_pack_name})")

                    # Get share pack from database
                    from uuid import UUID

                    from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

                    repo = SharePackRepository(db_pool.pool)
                    share_pack = await repo.get_current(UUID(share_pack_id))

                    if not share_pack:
                        logger.error(f"Share pack {share_pack_id} not found in database")
                        queue_client.delete_message(msg)
                        continue

                    # Call orchestrator based on strategy
                    from dbrx_api.workflow.orchestrator.provisioning import provision_sharepack_new
                    from dbrx_api.workflow.orchestrator.provisioning_update import provision_sharepack_update

                    strategy = share_pack["strategy"].upper()
                    logger.info(f"Provisioning share pack {share_pack_id} with strategy: {strategy}")

                    if strategy == "NEW":
                        await provision_sharepack_new(
                            pool=db_pool.pool,
                            share_pack=share_pack,
                        )
                    elif strategy == "UPDATE":
                        await provision_sharepack_update(
                            pool=db_pool.pool,
                            share_pack=share_pack,
                        )
                    else:
                        logger.error(f"Unknown strategy '{strategy}' for {share_pack_id}")
                        await repo.update_status(
                            UUID(share_pack_id),
                            "FAILED",
                            f"Unknown strategy: {strategy}",
                            f"Strategy must be NEW or UPDATE, got: {strategy}",
                            "orchestrator",
                        )

                    logger.success(f"Share pack {share_pack_id} provisioned successfully")

                    # Delete message to acknowledge processing
                    queue_client.delete_message(msg)

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # Message will become visible again for retry
                    # Don't delete - let it retry after visibility timeout

        except Exception as e:
            logger.error(f"Queue consumer error: {e}", exc_info=True)

        # Sleep between polls
        await asyncio.sleep(5)
