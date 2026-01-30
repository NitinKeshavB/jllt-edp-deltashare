"""Azure Blob Storage handler for loguru."""
import json
from datetime import timezone
from typing import Any
from typing import Optional

from loguru import logger

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    AZURE_SDK_AVAILABLE = True
except ImportError:
    logger.warning("Azure SDK not installed - blob logging will be disabled")
    DefaultAzureCredential = None  # type: ignore
    BlobServiceClient = None  # type: ignore
    AZURE_SDK_AVAILABLE = False


class AzureBlobLogHandler:
    """Handler to send logs to Azure Blob Storage."""

    def __init__(
        self,
        storage_account_url: str,
        container_name: str = "logging",
        sas_url: Optional[str] = None,
    ):
        """
        Initialize Azure Blob Storage handler.

        Args:
            storage_account_url: Azure Storage Account URL (e.g., https://<account>.blob.core.windows.net)
            container_name: Container name for logs (default: logging)
            sas_url: Azure Storage SAS URL (fallback if managed identity fails)
        """
        self.storage_account_url = storage_account_url
        self.container_name = container_name
        self.sas_url = sas_url
        self.blob_service_client = None
        self.container_client = None
        self._auth_method = None  # Track which auth method was used
        self._upload_count = 0
        self._failed_upload_count = 0
        self._last_error = None

        # Batching queue for scalable logging
        self._log_queue: Optional[Any] = None  # asyncio.Queue
        self._batch_size = 50  # Write batch when this many logs collected
        self._batch_timeout = 10.0  # Or after 10 seconds (blob uploads can be slower)
        self._max_queue_size = 5000  # Backpressure limit
        self._worker_task: Optional[Any] = None  # Background worker task
        self._circuit_breaker_failures = 0
        self._circuit_breaker_last_failure: Optional[float] = None
        self._circuit_breaker_state = "closed"  # closed, open, half-open
        self._circuit_breaker_threshold = 5  # Open after 5 consecutive failures
        self._circuit_breaker_timeout = 60.0  # Try again after 60 seconds

        # Throttle repeated permission/auth errors (avoid log spam)
        self._last_auth_error_log_time: Optional[float] = None
        self._auth_error_throttle_seconds = 300  # Only log auth errors once per 5 minutes

    def _extract_method_from_path(self, request_path: Optional[str]) -> Optional[str]:
        """Extract HTTP method from request_path like 'GET /api/shares'."""
        if not request_path:
            return None
        parts = request_path.split(" ", 1)
        return parts[0] if parts else None

    def _extract_path_from_request_path(self, request_path: Optional[str]) -> Optional[str]:
        """Extract URL path from request_path like 'GET /api/shares'."""
        if not request_path:
            return None
        parts = request_path.split(" ", 1)
        return parts[1] if len(parts) > 1 else None

    def _sanitize_sensitive_data(self, value: Optional[str]) -> Optional[str]:
        """
        Sanitize sensitive data like tokens before storing in blob storage.

        Patterns sanitized:
        - bearer_token:xxx... -> bearer_token:[REDACTED]
        - api_key:xxx... -> api_key:[REDACTED]
        - Authorization headers -> [REDACTED]
        """
        if not value:
            return value

        import re

        # Sanitize bearer token previews
        value = re.sub(r"bearer_token:[^\s]+", "bearer_token:[REDACTED]", value, flags=re.IGNORECASE)

        # Sanitize API key previews
        value = re.sub(r"api_key:[^\s]+", "api_key:[REDACTED]", value, flags=re.IGNORECASE)

        # Sanitize any JWT-like tokens (base64 with dots)
        value = re.sub(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[JWT_REDACTED]", value)

        # Sanitize Bearer tokens in headers
        value = re.sub(r"Bearer\s+[A-Za-z0-9_-]+", "Bearer [REDACTED]", value, flags=re.IGNORECASE)

        return value

    def _truncate_body(self, body: Any, max_length: int) -> Any:
        """
        Truncate request/response body to max_length characters while preserving important details.

        Args:
            body: Request/response body (dict, list, str, or other)
            max_length: Maximum length in characters (200)

        Returns:
            Truncated body with preview and metadata
        """
        if body is None:
            return None

        try:
            # Convert to JSON string to measure actual length
            body_str = json.dumps(body, default=str, ensure_ascii=False)
            original_size = len(body_str)

            # If within limit, return as-is
            if original_size <= max_length:
                return body

            # Truncate to fit max_length, leaving room for metadata
            preview_length = max_length - 60
            if preview_length < 50:  # Ensure minimum preview
                preview_length = 50

            truncated_preview = body_str[:preview_length]

            return {
                "_truncated": True,
                "_original_size": original_size,
                "_preview": truncated_preview,
            }

        except Exception:
            # If JSON conversion fails, convert to string and truncate
            body_str = str(body)
            original_size = len(body_str)
            if original_size <= max_length:
                return body

            preview_length = max_length - 60
            if preview_length < 50:
                preview_length = 50

            return {
                "_truncated": True,
                "_original_size": original_size,
                "_preview": body_str[:preview_length],
            }

    def _sanitize_dict(self, data: Any) -> Any:
        """
        Recursively sanitize sensitive data in dictionaries and lists.

        Args:
            data: Dictionary, list, or primitive value to sanitize

        Returns:
            Sanitized data with sensitive values redacted
        """
        if data is None:
            return None

        if isinstance(data, dict):
            sanitized = {}
            for key, value in data.items():
                # Recursively sanitize nested structures
                sanitized[key] = self._sanitize_dict(value)
            return sanitized

        if isinstance(data, list):
            return [self._sanitize_dict(item) for item in data]

        if isinstance(data, str):
            # Sanitize string values
            return self._sanitize_sensitive_data(data)

        # Return primitives as-is
        return data

    def sink(self, message: Any) -> None:
        """
        Loguru sink function to write logs to Azure Blob Storage.
        Never raises so loguru does not print sink exception messages.

        Args:
            message: Log message from loguru
        """
        try:
            self._sink_impl(message)
        except Exception as e:
            # Never let sink raise - prevents loguru from printing "Error writing log to ..."
            import time

            err_str = str(e)
            is_auth_error = "authorization" in err_str.lower() or "permission" in err_str.lower()
            if is_auth_error:
                now = time.time()
                if (
                    self._last_auth_error_log_time is None
                    or (now - self._last_auth_error_log_time) >= self._auth_error_throttle_seconds
                ):
                    self._last_auth_error_log_time = now
                    print(
                        f"⚠ Azure Blob Storage: permission/authorization error (throttled). "
                        f"Check SAS URL or RBAC (Storage Blob Data Contributor). {err_str[:150]}",
                        flush=True,
                    )
            else:
                print(
                    f"⚠ Azure Blob Storage logging skipped: {err_str[:200]}",
                    flush=True,
                )

    def _sink_impl(self, message: Any) -> None:
        """Actual sink logic; called from sink() so exceptions are caught."""
        # Skip initialization messages to avoid recursion and blocking
        record = message.record
        log_message = str(record.get("message", ""))
        if any(
            keyword in log_message
            for keyword in [
                "Azure Blob Storage",
                "blob logging",
                "blob storage",
                "Configuration loaded",
                "Starting DeltaShare",
            ]
        ):
            return

        # Lazy initialization - only initialize on first actual application log
        # This prevents blocking during app startup
        if not self.blob_service_client:
            try:
                if not self._ensure_container():
                    # Initialization failed, skip this log but will retry on next one
                    # Log to console so it appears in web app logs for review
                    record = message.record
                    log_msg = str(record.get("message", ""))[:100]
                    print(
                        f"⚠ Azure Blob Storage logging skipped: Initialization failed. "
                        f"Error: {self._last_error or 'Unknown error'}. "
                        f"Log message was: {log_msg}",
                        flush=True,
                    )
                    return
            except Exception as e:
                # Don't let initialization errors crash the app
                self._last_error = str(e)
                record = message.record
                log_msg = str(record.get("message", ""))[:100]
                print(
                    f"⚠ Azure Blob Storage logging skipped: Exception during initialization: {str(e)[:200]}. "
                    f"Log message was: {log_msg}",
                    flush=True,
                )
                return

        try:
            # Parse the log record
            record = message.record
            timestamp = record["time"].astimezone(timezone.utc)

            # Create blob name with date partitioning: YYYY/MM/DD/HH/logs_YYYYMMDD_HHmmss_uuid.json
            # Add UUID to prevent collisions
            import uuid

            blob_name = (
                f"{timestamp.year:04d}/"
                f"{timestamp.month:02d}/"
                f"{timestamp.day:02d}/"
                f"{timestamp.hour:02d}/"
                f"log_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex[:8]}.json"
            )

            # Handle extra data - may be dict or JSON string (if process_log_record ran first)
            raw_extra = record["extra"]
            if isinstance(raw_extra, str):
                try:
                    extra = json.loads(raw_extra)
                except (json.JSONDecodeError, TypeError):
                    extra = {}
            else:
                extra = raw_extra or {}

            # Extract HTTP context fields from extra data (set by RequestContextMiddleware)
            request_id = extra.get("request_id")
            # Try multiple field names for method (http_method from middleware/errors, method from middleware)
            http_method = (
                extra.get("http_method")
                or extra.get("method")
                or self._extract_method_from_path(extra.get("request_path"))
            )
            # Try multiple field names for path (url_path from middleware/errors, path from middleware)
            url_path = (
                extra.get("url_path")
                or extra.get("path")
                or self._extract_path_from_request_path(extra.get("request_path"))
            )
            # Try multiple field names for status code (http_status from middleware, status_code from error handlers)
            status_code = extra.get("http_status") or extra.get("status_code")
            if status_code is not None:
                try:
                    status_code = int(status_code)
                except (ValueError, TypeError):
                    status_code = None
            response_time_ms = extra.get("response_time_ms")
            client_ip = extra.get("client_ip")
            user_identity = self._sanitize_sensitive_data(extra.get("user_identity"))
            user_agent = extra.get("user_agent")

            # Extract and process request/response bodies
            request_body = extra.get("request_body")
            response_body = extra.get("response_body")

            # Truncate and sanitize request/response bodies (200 char limit)
            MAX_BODY_LENGTH = 200
            if request_body is not None:
                request_body = self._truncate_body(request_body, MAX_BODY_LENGTH)
                request_body = self._sanitize_dict(request_body)
            if response_body is not None:
                response_body = self._truncate_body(response_body, MAX_BODY_LENGTH)
                response_body = self._sanitize_dict(response_body)

            # Prepare log entry with structured HTTP context
            # Handle both real loguru records and test mocks
            level = record["level"]["name"] if isinstance(record["level"], dict) else record["level"].name
            log_entry = {
                "timestamp": timestamp.isoformat(),
                "level": level,
                "logger": record["name"],
                "function": record["function"],
                "line": record["line"],
                "message": record["message"],
                # HTTP Request Context
                "http": {
                    "request_id": request_id,
                    "method": http_method,
                    "url_path": url_path,
                    "status_code": status_code,
                    "response_time_ms": response_time_ms,
                    "client_ip": client_ip,
                    "user_identity": user_identity,
                    "user_agent": user_agent,
                    "request_body": request_body,
                    "response_body": response_body,
                },
                # Other extra data (excluding HTTP fields to avoid duplication)
                "extra": {
                    k: v
                    for k, v in extra.items()
                    if k
                    not in (
                        "request_id",
                        "http_method",
                        "url_path",
                        "http_status",
                        "status_code",
                        "response_time_ms",
                        "client_ip",
                        "user_identity",
                        "user_agent",
                        "request_body",
                        "response_body",
                        "request_path",
                        "method",  # Also remove 'method' if present (from middleware)
                        "path",  # Also remove 'path' if present (from middleware)
                    )
                },
            }

            # Add exception info if present
            if record["exception"]:
                log_entry["exception"] = {
                    "type": str(record["exception"].type),
                    "value": str(record["exception"].value),
                    "traceback": record["exception"].traceback,
                }

            # Add to queue for batched processing (scalable design)
            import asyncio

            try:
                asyncio.get_running_loop()
                # Initialize queue and worker on first use
                if self._log_queue is None:
                    self._log_queue = asyncio.Queue(maxsize=self._max_queue_size)
                    # Start background worker
                    self._worker_task = asyncio.create_task(self._batch_worker())

                # Try to add to queue (non-blocking with backpressure)
                try:
                    self._log_queue.put_nowait((blob_name, log_entry))
                except asyncio.QueueFull:
                    # Queue is full - drop log to prevent memory growth
                    # Log to console so it appears in web app logs
                    log_msg = str(log_entry.get("message", "N/A"))[:100] if isinstance(log_entry, dict) else "N/A"
                    print(
                        f"⚠ Azure Blob Storage logging queue full ({self._max_queue_size} logs) - dropping log. "
                        f"Message: {log_msg}",
                        flush=True,
                    )
            except RuntimeError:
                # No running event loop - upload synchronously (fallback for non-async contexts)
                # This happens during app startup before FastAPI's loop starts
                self._upload_log_sync(blob_name, log_entry)

        except Exception as e:
            # Don't let logging errors crash the app or propagate to loguru
            err_str = str(e)
            is_auth_error = "authorization" in err_str.lower() or "permission" in err_str.lower()
            import time

            if is_auth_error:
                now = time.time()
                if (
                    self._last_auth_error_log_time is None
                    or (now - self._last_auth_error_log_time) >= self._auth_error_throttle_seconds
                ):
                    self._last_auth_error_log_time = now
                    print(
                        f"⚠ Azure Blob Storage: not authorized. Use SAS URL or grant RBAC "
                        f"(Storage Blob Data Contributor). {err_str[:150]}",
                        flush=True,
                    )
            else:
                print(
                    f"⚠ Azure Blob Storage logging skipped: {err_str[:200]}",
                    flush=True,
                )

    def _ensure_container(self) -> bool:
        """Initialize client and ensure container exists, creating it if needed.

        Tries SAS URL first (fastest, no hanging), falls back to managed identity if SAS URL not provided.
        """
        if not AZURE_SDK_AVAILABLE:
            self._last_error = "Azure SDK not available"
            return False

        # Initialize client (try SAS URL first, then managed identity)
        if not self.blob_service_client:
            # Try SAS URL first (fastest, no hanging)
            if self.sas_url:
                try:
                    sas_url_clean = self.sas_url.strip('"').strip("'")

                    # Extract account URL from SAS URL
                    url_parts = sas_url_clean.split("?")
                    base_url = url_parts[0].rstrip("/")
                    sas_token = url_parts[1] if len(url_parts) > 1 else ""

                    # Build account-level URL for BlobServiceClient
                    path_parts = base_url.split("/")
                    if len(path_parts) >= 4:
                        account_url = "/".join(path_parts[:4])  # https://<account>.blob.core.windows.net
                        sas_url_for_client = f"{account_url}?{sas_token}" if sas_token else account_url
                    else:
                        sas_url_for_client = sas_url_clean

                    # Create client from SAS URL
                    self.blob_service_client = BlobServiceClient(account_url=sas_url_for_client)  # type: ignore
                    self._auth_method = "sas_url"
                except Exception as sas_error:
                    self._last_error = f"Failed to initialize with SAS URL: {sas_error}"
                    return False
            else:
                # SAS URL not provided, try managed identity (only in Azure Web App)
                import os

                if DefaultAzureCredential is not None and os.getenv("WEBSITE_INSTANCE_ID"):
                    try:
                        credential = DefaultAzureCredential()  # type: ignore
                        self.blob_service_client = BlobServiceClient(  # type: ignore
                            account_url=self.storage_account_url, credential=credential
                        )
                        self._auth_method = "managed_identity"
                    except Exception as mi_error:
                        self._last_error = f"Managed identity failed: {mi_error}"
                        return False
                else:
                    self._last_error = "SAS URL not provided and managed identity not available (not in Azure Web App)"
                    return False

        # Ensure container client is ready (no network calls here - just create client object)
        if not self.container_client:
            self.container_client = self.blob_service_client.get_container_client(self.container_name)

        return True

    def _upload_log_sync(self, blob_name: str, log_entry: dict) -> None:
        """Synchronously upload log to blob storage."""
        if not self._ensure_container():
            self._failed_upload_count += 1
            return

        try:
            # Ensure container exists (check/create on first upload, not during initialization)
            if not self.container_client:
                self.container_client = self.blob_service_client.get_container_client(self.container_name)

            # Try to create container if it doesn't exist (with timeout to prevent hanging)
            try:
                import os
                import signal

                # Set timeout for container operations (3 seconds)
                def timeout_handler(signum, frame):
                    raise TimeoutError("Container operation timed out")

                # Only set timeout on Unix systems
                if hasattr(signal, "SIGALRM") and os.name != "nt":
                    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(3)  # 3 second timeout
                    try:
                        if not self.container_client.exists():
                            self.container_client.create_container()
                    finally:
                        signal.alarm(0)  # Cancel timeout
                        signal.signal(signal.SIGALRM, old_handler)
                else:
                    # Windows or no signal support - just try without timeout
                    if not self.container_client.exists():
                        self.container_client.create_container()
            except TimeoutError:
                # Container check timed out - continue anyway, upload might still work
                pass
            except Exception:
                # Container might already exist or we don't have permission to check
                pass

            # Upload with timeout protection (Azure SDK should handle timeouts internally)
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
            log_content = json.dumps(log_entry, default=str, indent=2)
            blob_client.upload_blob(log_content, overwrite=False, content_type="application/json")
            self._upload_count += 1
        except Exception as e:
            self._failed_upload_count += 1
            self._last_error = str(e)
            err_str = str(e)
            is_auth_error = "authorization" in err_str.lower() or "permission" in err_str.lower()
            import time

            # Throttle auth/permission errors to avoid log spam
            if is_auth_error:
                now = time.time()
                if (
                    self._last_auth_error_log_time is None
                    or (now - self._last_auth_error_log_time) >= self._auth_error_throttle_seconds
                ):
                    self._last_auth_error_log_time = now
                    print(
                        "⚠ Azure Blob Storage: not authorized. Use SAS URL with write permissions "
                        "or grant RBAC 'Storage Blob Data Contributor' to the Web App identity.",
                        flush=True,
                    )
            else:
                log_msg = str(log_entry.get("message", ""))[:100] if isinstance(log_entry, dict) else "N/A"
                print(
                    f"⚠ Azure Blob Storage logging skipped: Upload failed: {err_str[:200]}. Log message: {log_msg}",
                    flush=True,
                )

    async def _batch_worker(self) -> None:
        """
        Background worker that processes logs in batches for scalability.

        Collects logs from queue and uploads them in batches to reduce blob storage overhead.
        """
        import asyncio
        import time

        batch = []
        last_write_time = time.time()

        while True:
            try:
                # Wait for log with timeout (to flush partial batches)
                timeout = self._batch_timeout - (time.time() - last_write_time)
                timeout = max(0.1, min(timeout, self._batch_timeout))

                try:
                    blob_name, log_entry = await asyncio.wait_for(self._log_queue.get(), timeout=timeout)
                    batch.append((blob_name, log_entry))
                except asyncio.TimeoutError:
                    # Timeout - upload partial batch if we have logs
                    pass

                # Upload batch if:
                # 1. Batch size reached, OR
                # 2. Timeout elapsed
                should_upload = len(batch) >= self._batch_size or (
                    batch and (time.time() - last_write_time) >= self._batch_timeout
                )

                if should_upload and batch:
                    await self._upload_batch(batch)
                    batch = []
                    last_write_time = time.time()

            except Exception as e:
                # Don't let worker errors crash the app
                print(f"Error in Azure Blob Storage batch worker: {e}", flush=True)
                await asyncio.sleep(1)  # Brief pause before retrying

    async def _upload_batch(self, batch: list[tuple[str, dict]]) -> None:
        """
        Upload a batch of logs to Azure Blob Storage.

        For now, uploads individually (Azure Blob doesn't support batch uploads natively).
        In future, could combine multiple logs into single JSON file.

        Args:
            batch: List of (blob_name, log_entry) tuples
        """
        if not batch:
            return

        # Check circuit breaker
        import time

        if self._circuit_breaker_state == "open":
            if time.time() - (self._circuit_breaker_last_failure or 0) < self._circuit_breaker_timeout:
                # Circuit is open, skip batch
                print(
                    f"⚠ Azure Blob Storage logging skipped: Circuit breaker is OPEN " f"({len(batch)} logs dropped)",
                    flush=True,
                )
                return
            else:
                # Timeout elapsed, try again (half-open)
                self._circuit_breaker_state = "half-open"

        # Ensure container is ready
        if not self._ensure_container():
            self._circuit_breaker_failures += 1
            self._circuit_breaker_last_failure = time.time()
            if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
                self._circuit_breaker_state = "open"
            print(
                f"⚠ Azure Blob Storage logging skipped: Container initialization failed. "
                f"Batch of {len(batch)} logs dropped",
                flush=True,
            )
            return

        # Upload each log in batch (Azure Blob doesn't support true batch uploads)
        # But we do this in parallel using asyncio.gather for efficiency
        import asyncio

        async def upload_one(blob_name: str, log_entry: dict) -> None:
            """Upload a single log entry."""
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._upload_log_sync, blob_name, log_entry)
            except Exception:
                pass  # Errors already handled in _upload_log_sync

        try:
            # Upload all logs in batch concurrently (up to 10 at a time to avoid overwhelming)
            tasks = [upload_one(blob_name, log_entry) for blob_name, log_entry in batch]
            # Process in chunks of 10 to avoid too many concurrent uploads
            chunk_size = 10
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i : i + chunk_size]
                await asyncio.gather(*chunk, return_exceptions=True)

            # Success - reset circuit breaker
            if self._circuit_breaker_state == "half-open":
                self._circuit_breaker_state = "closed"
            self._circuit_breaker_failures = 0
        except Exception as e:
            # Failure - update circuit breaker
            self._circuit_breaker_failures += 1
            self._circuit_breaker_last_failure = time.time()
            if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
                self._circuit_breaker_state = "open"
            print(
                f"⚠ Azure Blob Storage batch upload failed: {str(e)[:200]}. "
                f"Batch of {len(batch)} logs partially uploaded",
                flush=True,
            )

    def test_upload(self) -> dict:
        """
        Test upload a sample log entry to verify blob storage is working.

        Returns:
            Dictionary with test results including success status and error details
        """
        import uuid
        from datetime import datetime

        test_log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "logger": "test",
            "function": "test_upload",
            "line": 0,
            "message": "Test log entry to verify blob storage logging",
            "http": {
                "request_id": f"test-{uuid.uuid4().hex[:8]}",
                "method": "GET",
                "url_path": "/health/logging/test",
                "status_code": 200,
                "response_time_ms": 0.0,
                "client_ip": "127.0.0.1",
                "user_identity": "test-user",
                "user_agent": "test-agent",
                "request_body": None,
                "response_body": {"test": "data"},
            },
            "extra": {"test": True},
        }

        test_blob_name = (
            f"test/test_log_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
        )

        result = {
            "success": False,
            "blob_name": test_blob_name,
            "container": self.container_name,
            "error": None,
            "details": {},
        }

        try:
            # Force synchronous upload for testing
            self._upload_log_sync(test_blob_name, test_log_entry)

            # Check if upload was successful
            if self._upload_count > 0:
                result["success"] = True
                result["details"] = {
                    "upload_count": self._upload_count,
                    "failed_upload_count": self._failed_upload_count,
                    "last_error": self._last_error,
                }
            else:
                result["error"] = self._last_error or "Upload failed but no error recorded"
                result["details"] = {
                    "upload_count": self._upload_count,
                    "failed_upload_count": self._failed_upload_count,
                }
        except Exception as e:
            result["error"] = str(e)
            import traceback

            result["details"] = {"traceback": traceback.format_exc()}

        return result

    def verify_logging_status(self) -> dict:
        """Verify if logging to blob storage is working."""
        # Get queue status if available
        queue_size = 0
        worker_running = False
        if self._log_queue is not None:
            try:
                queue_size = self._log_queue.qsize()
            except Exception:
                pass
        if self._worker_task is not None:
            try:
                worker_running = not self._worker_task.done()
            except Exception:
                pass

        status = {
            "container_name": self.container_name,
            "storage_account_url": self.storage_account_url,
            "client_initialized": self.blob_service_client is not None,
            "container_client_initialized": self.container_client is not None,
            "auth_method": self._auth_method,
            "upload_count": self._upload_count,
            "failed_upload_count": self._failed_upload_count,
            "last_error": self._last_error,
            "sas_url_provided": self.sas_url is not None,
            "queue_size": queue_size,  # Number of logs waiting to be uploaded
            "worker_running": worker_running,  # Is batch worker running
            "batch_size": self._batch_size,  # Logs per batch
            "batch_timeout": self._batch_timeout,  # Seconds before flushing partial batch
        }

        # Check container and blob count if client is initialized
        if self.blob_service_client:
            try:
                if not self.container_client:
                    self.container_client = self.blob_service_client.get_container_client(self.container_name)

                status["container_exists"] = self.container_client.exists()

                if status["container_exists"]:
                    blob_list = list(self.container_client.list_blobs(max_results=10))
                    status["blob_count"] = len(blob_list)
                    status["sample_blobs"] = [blob.name for blob in blob_list[:5]]
                else:
                    status["blob_count"] = 0
                    status["sample_blobs"] = []
            except Exception as e:
                status["container_check_error"] = str(e)
                status["container_exists"] = False
                status["blob_count"] = 0
        else:
            status["container_exists"] = False
            status["blob_count"] = 0

        return status

    def __call__(self, message: Any) -> None:
        """Allow handler to be called directly."""
        self.sink(message)
