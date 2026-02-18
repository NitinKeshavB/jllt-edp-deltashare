"""
Queue Consumer for Share Pack Processing

Background task that polls the queue and triggers provisioning.
"""

import asyncio
import json

from loguru import logger


def is_retryable_error(error: Exception) -> bool:
    """
    Determine if an error is transient and should be retried.

    Retryable errors (transient failures):
    - Timeout errors (ReadTimeout, ConnectionTimeout, TimeoutError)
    - Network errors (ConnectionError, ConnectionResetError)
    - HTTP 503 Service Unavailable
    - HTTP 504 Gateway Timeout
    - Database connection errors

    Non-retryable errors (permanent failures):
    - ValueError (validation errors, immutable field changes)
    - RuntimeError (pipeline/share/recipient failures)
    - PermissionError, KeyError
    - HTTP 4xx errors (except 429, 503, 504)

    Args:
        error: The exception to check

    Returns:
        True if error should be retried, False otherwise
    """
    error_str = str(error).lower()
    error_type = type(error).__name__

    # Check for timeout-related errors
    if "timeout" in error_type.lower() or "timeout" in error_str:
        return True

    # Check for connection-related errors
    if "connection" in error_type.lower() or "connection" in error_str:
        return True

    # Check for network errors
    if error_type in ["ConnectionError", "ConnectionResetError", "BrokenPipeError"]:
        return True

    # Check for HTTP 503/504 errors
    if (
        "503" in error_str
        or "504" in error_str
        or "service unavailable" in error_str
        or "gateway timeout" in error_str
    ):
        return True

    # Check for rate limiting (429)
    if "429" in error_str or "rate limit" in error_str or "too many requests" in error_str:
        return True

    # Check for database connection errors
    if "database" in error_str and ("connection" in error_str or "timeout" in error_str):
        return True

    # Non-retryable errors - fail immediately
    if error_type in ["ValueError", "RuntimeError", "PermissionError", "KeyError", "TypeError"]:
        return False

    # Default: don't retry unknown errors
    return False


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

                    # Call orchestrator based on strategy with retry logic
                    from dbrx_api.workflow.orchestrator.provisioning import provision_sharepack_new
                    from dbrx_api.workflow.orchestrator.provisioning_delete import provision_sharepack_delete
                    from dbrx_api.workflow.orchestrator.provisioning_update import provision_sharepack_update

                    strategy = (share_pack.get("strategy") or "NEW").strip().upper()
                    logger.info(f"Provisioning share pack {share_pack_id} with strategy: {strategy}")

                    # Retry logic - retry only once on failure (2 total attempts)
                    max_retries = 1
                    retry_count = 0
                    last_error = None

                    while retry_count <= max_retries:
                        try:
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
                            elif strategy == "DELETE":
                                await provision_sharepack_delete(
                                    pool=db_pool.pool,
                                    share_pack=share_pack,
                                )
                            else:
                                logger.error(f"Unknown strategy '{strategy}' for {share_pack_id}")
                                await repo.update_status(
                                    UUID(share_pack_id),
                                    "FAILED",
                                    f"Unknown strategy: {strategy}",
                                    f"Strategy must be NEW, UPDATE, or DELETE, got: {strategy}",
                                    "orchestrator",
                                )
                                break

                            logger.success(f"Share pack {share_pack_id} provisioned successfully")
                            break  # Success - exit retry loop (no retry on success)

                        except Exception as prov_error:
                            last_error = prov_error

                            # Check if error is retryable (timeout/network errors only)
                            is_retryable = is_retryable_error(prov_error)

                            if not is_retryable:
                                # Non-retryable: orchestrator already called tracker.fail() with real
                                # error and step - do not overwrite status here (preserves accurate
                                # ErrorMessage and ProvisioningStatus for GET sharepack status).
                                logger.error(f"Non-retryable error detected - failing immediately: {prov_error}")
                                raise  # Re-raise to outer exception handler

                            # Retryable error - check if we have retries left
                            retry_count += 1

                            if retry_count <= max_retries:
                                # Retryable error and retries available
                                logger.warning(
                                    f"Provisioning failed with retryable error (attempt {retry_count}/{max_retries + 1}): {prov_error}"
                                )
                                logger.info(
                                    f"Waiting 10 minutes before retrying provisioning for share pack {share_pack_id}..."
                                )
                                await asyncio.sleep(600)  # Wait 10 minutes (600 seconds) before retry
                            else:
                                # Retries exhausted: orchestrator already called tracker.fail() with
                                # real error - do not overwrite status here.
                                logger.error(f"Retried failed request and stopping. Last error: {last_error}")
                                raise  # Re-raise to outer exception handler

                    # Delete message to acknowledge processing
                    queue_client.delete_message(msg)

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # Message will become visible again for retry
                    # Don't delete - let it retry after visibility timeout

        except asyncio.CancelledError:
            logger.info("Queue consumer task cancelled - shutting down")
            raise
        except Exception as e:
            logger.error(f"Queue consumer error: {e}", exc_info=True)

        # Sleep between polls
        await asyncio.sleep(5)
