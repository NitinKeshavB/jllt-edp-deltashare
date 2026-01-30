"""PostgreSQL handler for loguru - stores critical logs in database."""
import json
from datetime import timezone
from typing import Any
from typing import Optional

from loguru import logger

try:
    import asyncpg
    from asyncpg import Pool

    ASYNCPG_AVAILABLE = True
except ImportError:
    logger.warning("asyncpg not installed - PostgreSQL logging will be disabled")
    asyncpg = None  # type: ignore
    Pool = None  # type: ignore
    ASYNCPG_AVAILABLE = False


class PostgreSQLLogHandler:
    """Handler to send critical logs to PostgreSQL database."""

    def __init__(
        self,
        connection_string: str,
        table_name: str = "application_logs",
        min_level: str = "WARNING",
    ):
        """
        Initialize PostgreSQL handler.

        Args:
            connection_string: PostgreSQL connection string
            table_name: Table name for logs (default: application_logs)
            min_level: Minimum log level to store (default: WARNING)
        """
        self.connection_string = connection_string
        self.table_name = table_name
        self.min_level = min_level
        self.pool: Optional[Pool] = None
        self._pool_init_lock: Optional[any] = None  # Async lock for pool initialization
        self._level_priority = {
            "TRACE": 0,
            "DEBUG": 1,
            "INFO": 2,
            "SUCCESS": 3,
            "WARNING": 4,
            "ERROR": 5,
            "CRITICAL": 6,
        }
        self._min_priority = self._level_priority.get(min_level, 4)

        # Batching queue for scalable logging
        self._log_queue: Optional[Any] = None  # asyncio.Queue
        self._batch_size = 50  # Write batch when this many logs collected
        self._batch_timeout = 5.0  # Or after 5 seconds
        self._max_queue_size = 5000  # Backpressure limit
        self._worker_task: Optional[Any] = None  # Background worker task
        self._circuit_breaker_failures = 0
        self._circuit_breaker_last_failure: Optional[float] = None
        self._circuit_breaker_state = "closed"  # closed, open, half-open
        self._circuit_breaker_threshold = 5  # Open after 5 consecutive failures
        self._circuit_breaker_timeout = 60.0  # Try again after 60 seconds

        # Retry queue for failed batches (to prevent log loss)
        self._retry_queue: Optional[Any] = None  # asyncio.Queue for failed batches
        self._retry_worker_task: Optional[Any] = None  # Background retry worker
        self._max_retry_attempts = 3  # Retry failed batches up to 3 times
        self._retry_delay = 30.0  # Wait 30 seconds before retrying failed batches

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
        Sanitize sensitive data like tokens before storing in database.

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

        Simple approach: truncate the JSON string representation to max_length and store as preview.

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
            # Reserve ~60 chars for metadata: {"_truncated":true,"_original_size":12345,"_preview":"..."}
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

    async def _ensure_pool(self, retry_count: int = 0) -> None:
        """
        Ensure asyncpg connection pool is initialized with improved timeout handling.

        Uses retry logic with exponential backoff for better resilience.

        Args:
            retry_count: Number of retries attempted (for recursive retries)
        """
        if not ASYNCPG_AVAILABLE:
            logger.warning("asyncpg not available - PostgreSQL logging disabled")
            return

        max_retries = 2  # Retry up to 2 times
        if retry_count > max_retries:
            logger.error(
                f"PostgreSQL connection failed after {max_retries} retries - " "logging disabled (app continues)"
            )
            self.pool = None
            return

        try:
            import asyncio
            import time

            # Improved timeout settings for better reliability
            # connection_timeout: max time to establish a single connection (15 seconds)
            # command_timeout: max time for queries (60 seconds)
            # Pool creation timeout: 30 seconds (allows time for multiple connection attempts)
            pool_start_time = time.time()

            self.pool = await asyncio.wait_for(
                asyncpg.create_pool(
                    self.connection_string,
                    min_size=1,  # Start with 1 connection
                    max_size=5,  # Allow up to 5 connections
                    command_timeout=60,  # Increased query timeout to 60 seconds
                    timeout=15,  # Increased connection timeout to 15 seconds
                    max_inactive_connection_lifetime=300,  # Close idle connections after 5 min
                    max_queries=50000,  # Recycle connections after 50k queries
                ),
                timeout=30.0,  # Increased overall timeout for pool creation to 30 seconds
            )

            pool_init_time = time.time() - pool_start_time

            # Validate pool connection with a simple query
            try:
                # Use pool.acquire() as async context manager with timeout
                # This ensures proper connection release
                async def acquire_and_validate():
                    async with self.pool.acquire() as conn:
                        # Test connection with a simple query
                        await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=5.0)

                await asyncio.wait_for(acquire_and_validate(), timeout=10.0)  # Total timeout for acquire + validate

                logger.info(
                    f"PostgreSQL logging pool initialized and validated in {pool_init_time:.2f}s",
                    pool_size=f"{self.pool.get_size()}/{self.pool.get_max_size()}",
                )
            except (asyncio.TimeoutError, Exception) as e:
                # Pool created but connection test failed - close pool and retry
                logger.warning(
                    f"PostgreSQL pool created but connection validation failed: {e}. " "Closing pool and retrying..."
                )
                if self.pool:
                    await self.pool.close()
                    self.pool = None
                # Retry if we haven't exceeded max retries
                if retry_count < max_retries:
                    wait_time = 2**retry_count
                    await asyncio.sleep(wait_time)
                    await self._ensure_pool(retry_count=retry_count + 1)
                return

            # Create table if it doesn't exist (with increased timeout)
            await asyncio.wait_for(self._create_table_if_not_exists(), timeout=10.0)  # Increased from 5 to 10 seconds

        except asyncio.TimeoutError:
            # Retry with exponential backoff
            if retry_count < max_retries:
                wait_time = 2**retry_count  # 1s, 2s, 4s
                logger.warning(
                    f"PostgreSQL connection timeout (attempt {retry_count + 1}/{max_retries + 1}). "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                await self._ensure_pool(retry_count=retry_count + 1)
            else:
                logger.error(
                    f"PostgreSQL connection timeout after {max_retries + 1} attempts - "
                    "logging disabled (app continues)"
                )
                self.pool = None
        except Exception as e:
            # Retry on other connection errors too
            if retry_count < max_retries and "connection" in str(e).lower():
                wait_time = 2**retry_count
                logger.warning(
                    f"PostgreSQL connection error (attempt {retry_count + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                await self._ensure_pool(retry_count=retry_count + 1)
            else:
                logger.error(f"Failed to initialize PostgreSQL logging pool: {e} - app continues")
                self.pool = None

    async def initialize_pool(self) -> None:
        """Initialize asyncpg connection pool (alias for _ensure_pool)."""
        await self._ensure_pool()

    async def _create_table_if_not_exists(self) -> None:
        """Create logs table if it doesn't exist."""
        if not self.pool:
            return

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            level VARCHAR(20) NOT NULL,
            message TEXT NOT NULL,

            -- HTTP Request Context (dedicated columns for easy querying)
            request_id UUID,
            http_method VARCHAR(10),
            url_path VARCHAR(500),
            status_code SMALLINT,
            client_ip VARCHAR(45),
            user_identity VARCHAR(255),
            user_agent TEXT,

            -- Request/Response Bodies (for debugging and auditing)
            request_body JSONB,
            response_body JSONB,

            -- Code Location
            logger_name VARCHAR(255),
            function_name VARCHAR(255),

            -- Exception Details
            exception_type VARCHAR(255),
            exception_value TEXT,
            exception_traceback TEXT,

            -- Flexible storage for additional data
            extra_data JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- Create indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_timestamp ON {self.table_name}(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_level ON {self.table_name}(level);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_status_code ON {self.table_name}(status_code);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_request_id ON {self.table_name}(request_id);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_http_method ON {self.table_name}(http_method);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_client_ip ON {self.table_name}(client_ip);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_user_identity ON {self.table_name}(user_identity);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_created_at ON {self.table_name}(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_extra_data ON {self.table_name} USING GIN(extra_data);
        """

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(create_table_sql)
            logger.info(f"Application logs table '{self.table_name}' ready")
        except Exception as e:
            logger.error(f"Failed to create logs table: {e}")

    def sink(self, message: Any) -> None:
        """
        Loguru sink function to write logs to PostgreSQL.

        Note: This is a synchronous wrapper - actual DB write happens async in background.
        Pool is lazily initialized on first use to ensure it's created in the correct event loop.

        Args:
            message: Log message from loguru
        """
        try:
            record = message.record

            # Check if log level meets minimum threshold
            # Handle both real loguru records and test mocks
            level_name = record["level"]["name"] if isinstance(record["level"], dict) else record["level"].name
            level_priority = self._level_priority.get(level_name, 0)
            if level_priority < self._min_priority:
                return

            timestamp = record["time"].astimezone(timezone.utc)

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
            http_method = extra.get("http_method") or self._extract_method_from_path(extra.get("request_path"))
            url_path = extra.get("url_path") or self._extract_path_from_request_path(extra.get("request_path"))
            # Try multiple field names for status code (http_status from middleware, status_code from error handlers)
            status_code = extra.get("http_status") or extra.get("status_code")
            # Ensure status_code is an integer if present
            if status_code is not None:
                try:
                    status_code = int(status_code)
                except (ValueError, TypeError):
                    status_code = None
            extra.get("response_time_ms")
            client_ip = extra.get("client_ip")
            # Sanitize user_identity to remove token previews
            user_identity = self._sanitize_sensitive_data(extra.get("user_identity"))
            user_agent = extra.get("user_agent")

            # Extract request/response bodies (sanitized and truncated)
            request_body = extra.get("request_body")
            response_body = extra.get("response_body")

            # Truncate and sanitize request/response bodies to avoid clogging database
            # Limit to 200 chars while preserving important details
            MAX_BODY_LENGTH = 200
            if request_body is not None:
                request_body = self._truncate_body(request_body, MAX_BODY_LENGTH)
                request_body = self._sanitize_dict(request_body)
                # Convert to JSON string for JSONB column (asyncpg expects JSON strings, not dicts)
                request_body = json.dumps(request_body, default=str) if request_body is not None else None
            if response_body is not None:
                response_body = self._truncate_body(response_body, MAX_BODY_LENGTH)
                response_body = self._sanitize_dict(response_body)
                # Convert to JSON string for JSONB column (asyncpg expects JSON strings, not dicts)
                response_body = json.dumps(response_body, default=str) if response_body is not None else None

            # Remove extracted fields from extra_data to avoid duplication
            extra_for_json = {
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
            }

            # Prepare log data with dedicated columns
            log_data = {
                "timestamp": timestamp,
                "level": level_name,
                "message": record["message"],
                # HTTP Context
                "request_id": request_id,
                "http_method": http_method,
                "url_path": url_path,
                "status_code": status_code,
                "client_ip": client_ip,
                "user_identity": user_identity,
                "user_agent": user_agent,
                # Request/Response Bodies (sanitized)
                "request_body": request_body,
                "response_body": response_body,
                # Code Location
                "logger_name": record["name"],
                "function_name": record["function"],
                # Exception (filled below if present)
                "exception_type": None,
                "exception_value": None,
                "exception_traceback": None,
                # Remaining extra data (sanitized to remove any tokens)
                "extra_data": self._sanitize_sensitive_data(json.dumps(extra_for_json, default=str))
                if extra_for_json
                else None,
            }

            # Add exception info if present
            if record["exception"]:
                log_data["exception_type"] = str(record["exception"].type.__name__)
                log_data["exception_value"] = str(record["exception"].value)
                # Store formatted traceback
                import traceback as tb

                log_data["exception_traceback"] = "".join(
                    tb.format_exception(
                        record["exception"].type, record["exception"].value, record["exception"].traceback
                    )
                )

            # Add to queue for batched processing (scalable design)
            import asyncio

            try:
                asyncio.get_running_loop()
                # Initialize queue and worker on first use
                if self._log_queue is None:
                    self._log_queue = asyncio.Queue(maxsize=self._max_queue_size)
                    # Start background worker
                    self._worker_task = asyncio.create_task(self._batch_worker())

                # Initialize retry queue and worker on first use
                if self._retry_queue is None:
                    self._retry_queue = asyncio.Queue(maxsize=self._max_queue_size)
                    # Start background retry worker
                    self._retry_worker_task = asyncio.create_task(self._retry_worker())

                # Try to add to queue (non-blocking with backpressure)
                try:
                    self._log_queue.put_nowait(log_data)
                except asyncio.QueueFull:
                    # Queue is full - drop log to prevent memory growth
                    # Log to console so it appears in web app logs
                    print(
                        f"⚠ PostgreSQL logging queue full ({self._max_queue_size} logs) - dropping log. "
                        f"Message: {log_data.get('message', 'N/A')[:100]}",
                        flush=True,
                    )
            except RuntimeError:
                # No running event loop - skip database logging
                # This happens during app startup before FastAPI's loop starts
                pass

        except Exception as e:
            # Don't let logging errors crash the app
            print(f"Error preparing log for PostgreSQL: {e}", flush=True)

    async def _batch_worker(self) -> None:
        """
        Background worker that processes logs in batches for scalability.

        Collects logs from queue and writes them in batches to reduce database load.
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
                    log_data = await asyncio.wait_for(self._log_queue.get(), timeout=timeout)
                    batch.append(log_data)
                except asyncio.TimeoutError:
                    # Timeout - write partial batch if we have logs
                    pass

                # Write batch if:
                # 1. Batch size reached, OR
                # 2. Timeout elapsed
                should_write = len(batch) >= self._batch_size or (
                    batch and (time.time() - last_write_time) >= self._batch_timeout
                )

                if should_write and batch:
                    await self._write_batch(batch)
                    batch = []
                    last_write_time = time.time()

            except Exception as e:
                # Don't let worker errors crash the app
                print(f"Error in PostgreSQL batch worker: {e}", flush=True)
                await asyncio.sleep(1)  # Brief pause before retrying

    async def _write_batch(self, batch: list[dict]) -> None:
        """
        Write a batch of logs to PostgreSQL using batch INSERT for efficiency.

        Args:
            batch: List of log data dictionaries
        """
        if not batch:
            return

        # Check circuit breaker
        if self._circuit_breaker_state == "open":
            import time

            if time.time() - (self._circuit_breaker_last_failure or 0) < self._circuit_breaker_timeout:
                # Circuit is open, skip batch
                print(
                    f"⚠ PostgreSQL logging skipped: Circuit breaker is OPEN " f"({len(batch)} logs dropped)",
                    flush=True,
                )
                return
            else:
                # Timeout elapsed, try again (half-open)
                self._circuit_breaker_state = "half-open"

        # Ensure pool is initialized
        if not self.pool:
            import asyncio

            if self._pool_init_lock is None:
                self._pool_init_lock = asyncio.Lock()

            async with self._pool_init_lock:
                if not self.pool:
                    try:
                        # _ensure_pool now has its own timeout and retry logic
                        await self._ensure_pool()
                    except (asyncio.TimeoutError, Exception) as e:
                        self._circuit_breaker_failures += 1
                        self._circuit_breaker_last_failure = time.time()
                        if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
                            self._circuit_breaker_state = "open"
                        print(
                            f"⚠ PostgreSQL logging skipped: Pool initialization failed. "
                            f"Batch of {len(batch)} logs dropped",
                            flush=True,
                        )
                        return

        # Write batch using batch INSERT
        try:
            await self._insert_batch(batch)
            # Success - reset circuit breaker
            if self._circuit_breaker_state == "half-open":
                self._circuit_breaker_state = "closed"
            self._circuit_breaker_failures = 0
        except Exception as e:
            # Failure - add to retry queue instead of dropping
            import time

            self._circuit_breaker_failures += 1
            self._circuit_breaker_last_failure = time.time()
            if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
                self._circuit_breaker_state = "open"

            # Add failed batch to retry queue (don't lose logs!)
            if self._retry_queue is not None:
                try:
                    self._retry_queue.put_nowait((batch, 0))  # 0 = retry attempt count
                    print(
                        f"⚠ PostgreSQL batch write failed: {str(e)[:200]}. "
                        f"Batch of {len(batch)} logs queued for retry",
                        flush=True,
                    )
                except asyncio.QueueFull:
                    # Retry queue is full - log warning but don't crash
                    print(
                        f"⚠ PostgreSQL batch write failed and retry queue is full: {str(e)[:200]}. "
                        f"Batch of {len(batch)} logs may be lost",
                        flush=True,
                    )
            else:
                print(
                    f"⚠ PostgreSQL batch write failed: {str(e)[:200]}. "
                    f"Batch of {len(batch)} logs dropped (retry queue not initialized)",
                    flush=True,
                )

    async def _retry_worker(self) -> None:
        """
        Background worker that retries failed log batches.

        Prevents log loss by retrying batches that failed due to timeouts or errors.
        Uses exponential backoff to avoid overwhelming the database.
        """
        import asyncio

        while True:
            try:
                # Wait for failed batch in retry queue
                batch, retry_count = await self._retry_queue.get()

                # Wait before retrying (exponential backoff)
                wait_time = self._retry_delay * (2**retry_count)  # 30s, 60s, 120s
                await asyncio.sleep(wait_time)

                # Check if we've exceeded max retry attempts
                if retry_count >= self._max_retry_attempts:
                    print(
                        f"⚠ PostgreSQL batch retry exhausted after {retry_count + 1} attempts. "
                        f"Batch of {len(batch)} logs permanently lost",
                        flush=True,
                    )
                    continue

                # Try to write the batch again
                try:
                    await self._write_batch(batch)
                    print(
                        f"✓ PostgreSQL batch retry successful (attempt {retry_count + 1}). "
                        f"Batch of {len(batch)} logs inserted",
                        flush=True,
                    )
                except Exception as e:
                    # Retry failed again - add back to queue with incremented retry count
                    try:
                        self._retry_queue.put_nowait((batch, retry_count + 1))
                        print(
                            f"⚠ PostgreSQL batch retry failed (attempt {retry_count + 1}): {str(e)[:200]}. "
                            f"Will retry again later",
                            flush=True,
                        )
                    except asyncio.QueueFull:
                        # Retry queue is full - log warning
                        print(
                            f"⚠ PostgreSQL batch retry failed and retry queue is full. "
                            f"Batch of {len(batch)} logs may be lost after {retry_count + 1} attempts",
                            flush=True,
                        )

            except Exception as e:
                # Don't let retry worker errors crash the app
                print(f"Error in PostgreSQL retry worker: {e}", flush=True)
                await asyncio.sleep(5)  # Brief pause before retrying

    async def _insert_batch(self, batch: list[dict]) -> None:
        """
        Insert a batch of logs using efficient batch INSERT.

        Args:
            batch: List of log data dictionaries
        """
        if not self.pool or not batch:
            return

        import asyncio
        import uuid

        # Build batch INSERT query
        # Using VALUES with multiple rows for efficiency
        values_placeholders = []
        values_params = []
        param_index = 1

        for log_data in batch:
            # Convert request_id to UUID
            request_id = log_data.get("request_id")
            request_id_uuid = uuid.UUID(request_id) if request_id else None

            # Ensure request_body and response_body are JSON strings
            request_body = log_data.get("request_body")
            response_body = log_data.get("response_body")

            if request_body is not None and isinstance(request_body, dict):
                request_body = json.dumps(request_body, default=str)
            if response_body is not None and isinstance(response_body, dict):
                response_body = json.dumps(response_body, default=str)

            # Build placeholders for this row
            placeholders = ", ".join([f"${i}" for i in range(param_index, param_index + 18)])
            values_placeholders.append(f"({placeholders})")

            # Add parameters for this row
            values_params.extend(
                [
                    log_data["timestamp"],
                    log_data["level"],
                    log_data["message"],
                    request_id_uuid,
                    log_data.get("http_method"),
                    log_data.get("url_path"),
                    log_data.get("status_code"),
                    log_data.get("client_ip"),
                    log_data.get("user_identity"),
                    log_data.get("user_agent"),
                    request_body,
                    response_body,
                    log_data.get("logger_name"),
                    log_data.get("function_name"),
                    log_data.get("exception_type"),
                    log_data.get("exception_value"),
                    log_data.get("exception_traceback"),
                    log_data.get("extra_data"),
                ]
            )
            param_index += 18

        # Build final INSERT query
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            timestamp, level, message,
            request_id, http_method, url_path, status_code,
            client_ip, user_identity, user_agent,
            request_body, response_body,
            logger_name, function_name,
            exception_type, exception_value, exception_traceback,
            extra_data
        ) VALUES {', '.join(values_placeholders)}
        """

        # Execute batch insert with increased timeout for large batches
        # If timeout occurs, raise TimeoutError so batch can be retried
        async with self.pool.acquire() as conn:
            # Use longer timeout for batch inserts (60 seconds)
            # Batch size is 50, so this should be sufficient
            try:
                await asyncio.wait_for(
                    conn.execute(insert_sql, *values_params), timeout=60.0  # Increased to 60 seconds for batch insert
                )
            except asyncio.TimeoutError:
                # Re-raise timeout so it can be caught and retried
                raise

    async def _insert_log(self, log_data: dict) -> None:
        """
        Insert log entry into PostgreSQL.

        Args:
            log_data: Dictionary containing log data
        """
        if not self.pool:
            return

        insert_sql = f"""
        INSERT INTO {self.table_name} (
            timestamp, level, message,
            request_id, http_method, url_path, status_code,
            client_ip, user_identity, user_agent,
            request_body, response_body,
            logger_name, function_name,
            exception_type, exception_value, exception_traceback,
            extra_data
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        """

        try:
            # Convert request_id string to UUID if present
            import uuid

            request_id = log_data.get("request_id")
            request_id_uuid = uuid.UUID(request_id) if request_id else None

            # Convert request_body and response_body to JSON strings for JSONB columns
            # asyncpg's JSONB type expects JSON strings, not Python dicts
            request_body = log_data.get("request_body")
            response_body = log_data.get("response_body")

            # Ensure they're JSON strings (defensive - should already be strings from sink())
            if request_body is not None:
                if isinstance(request_body, dict):
                    request_body = json.dumps(request_body, default=str)
                elif not isinstance(request_body, str):
                    # If it's not a dict or string, convert to string
                    request_body = json.dumps(request_body, default=str)
            if response_body is not None:
                if isinstance(response_body, dict):
                    response_body = json.dumps(response_body, default=str)
                elif not isinstance(response_body, str):
                    # If it's not a dict or string, convert to string
                    response_body = json.dumps(response_body, default=str)

            import asyncio

            # Add timeout to prevent hanging on slow database writes
            async with self.pool.acquire() as conn:
                await asyncio.wait_for(
                    conn.execute(
                        insert_sql,
                        log_data["timestamp"],
                        log_data["level"],
                        log_data["message"],
                        request_id_uuid,
                        log_data.get("http_method"),
                        log_data.get("url_path"),
                        log_data.get("status_code"),
                        log_data.get("client_ip"),
                        log_data.get("user_identity"),
                        log_data.get("user_agent"),
                        request_body,  # Use converted value
                        response_body,  # Use converted value
                        log_data.get("logger_name"),
                        log_data.get("function_name"),
                        log_data.get("exception_type"),
                        log_data.get("exception_value"),
                        log_data.get("exception_traceback"),
                        log_data.get("extra_data"),
                    ),
                    timeout=30.0,  # Increased to 30 seconds for single insert
                )
        except asyncio.TimeoutError:
            # Log insert timeout to console so it appears in web app logs
            print(
                f"⚠ PostgreSQL logging skipped: Insert operation timed out (10s). "
                f"Log message was: {log_data.get('message', 'N/A')[:100]}",
                flush=True,
            )
        except Exception as e:
            # Log error to console so it appears in web app logs for review
            print(
                f"⚠ PostgreSQL logging skipped: {str(e)[:200]}. "
                f"Log message was: {log_data.get('message', 'N/A')[:100]}",
                flush=True,
            )

    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL logging pool closed")

    def __call__(self, message: Any) -> None:
        """Allow handler to be called directly."""
        self.sink(message)
