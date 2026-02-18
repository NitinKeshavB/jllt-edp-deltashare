"""Datadog handler for loguru - logging only."""
import asyncio
import json
import os
import traceback
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import Optional

import aiohttp
from loguru import logger


class DatadogLogHandler:
    """Handler to send logs to Datadog via HTTP API."""

    def __init__(
        self,
        api_key: str,
        service_name: str,
        env: Optional[str] = None,
        site: str = "us3.datadoghq.com",
    ):
        """
        Initialize Datadog handler.

        Args:
            api_key: Datadog API key (required)
            service_name: Service name (required, should be passed from settings)
            env: Environment name (auto-detected if not provided)
            site: Datadog site (defaults to us3.datadoghq.com)
        """
        self.api_key = api_key

        # Auto-detect environment if not provided
        if env is not None:
            self.env = env
        else:
            # Check if .env file exists for local development
            self.env = "local" if Path(".env").exists() else os.getenv("ENVIRONMENT", "unknown")
        self.service_name = service_name
        self.site = site
        self._session: Optional[aiohttp.ClientSession] = None
        self._endpoint: Optional[str] = None
        self._initialized = False
        self._initialize_datadog()

    def _initialize_datadog(self) -> None:
        """Initialize Datadog HTTP session."""
        if not self.api_key:
            logger.warning("Datadog API key not provided - Datadog logging will be disabled")
            return

        if not self.service_name:
            logger.warning("Datadog service name not provided - Datadog logging will be disabled")
            return

        try:
            self._endpoint = f"https://http-intake.logs.{self.site}/v1/input/{self.api_key}"
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                connector=aiohttp.TCPConnector(limit=10),
            )
            self._initialized = True
            logger.info(
                "Datadog logger initialized",
                service=self.service_name,
                env=self.env,
                site=self.site,
                endpoint=self._endpoint.split("/v1/input/")[0] + "/v1/input/***",
            )

        except Exception as e:
            logger.error(f"Failed to initialize Datadog logger: {e}", exc_info=True)
            self._initialized = False

    def sink(self, message: Any) -> None:
        """
        Loguru sink function to send logs to Datadog.

        Args:
            message: Log message from loguru
        """
        if not self._initialized or not self._session:
            return

        try:
            record = message.record
            timestamp = record["time"].astimezone(timezone.utc)

            level_name = record["level"]["name"] if isinstance(record["level"], dict) else record["level"].name
            level_mapping = {
                "TRACE": "debug",
                "DEBUG": "debug",
                "INFO": "info",
                "SUCCESS": "info",
                "WARNING": "warn",
                "ERROR": "error",
                "CRITICAL": "error",
            }
            dd_status = level_mapping.get(level_name, "info")
            payload = {
                "service": self.service_name,
                "ddsource": "python",
                "ddtags": f"env:{self.env}",
                "timestamp": int(timestamp.timestamp() * 1000),
                "status": dd_status.upper(),
                "message": record["message"],
                "logger": {
                    "name": record["name"],
                    "function": record["function"],
                    "line": record["line"],
                },
            }

            if record.get("extra"):
                extra = record["extra"]
                if isinstance(extra, dict):
                    for key, value in extra.items():
                        if key not in payload:
                            payload[key] = value
                elif isinstance(extra, str):
                    try:
                        parsed_extra = json.loads(extra)
                        if isinstance(parsed_extra, dict):
                            for key, value in parsed_extra.items():
                                if key not in payload:
                                    payload[key] = value
                    except (json.JSONDecodeError, TypeError):
                        payload["extra"] = extra

            if record.get("exception"):
                exc = record["exception"]
                exc_type = exc.type if hasattr(exc, "type") else type(exc)
                exc_value = exc.value if hasattr(exc, "value") else exc
                exc_traceback = exc.traceback if hasattr(exc, "traceback") else None

                payload["error"] = {
                    "kind": exc_type.__name__ if hasattr(exc_type, "__name__") else str(exc_type),
                    "message": str(exc_value),
                    "stack": "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                    if exc_traceback
                    else "",
                }

            try:
                asyncio.create_task(self._send_log(payload))
            except Exception as task_error:
                logger.error(f"Failed to create Datadog log task: {task_error}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in Datadog log handler sink: {e}", exc_info=True)

    async def _send_log(self, payload: dict) -> None:
        """Send log to Datadog HTTP API with retry logic."""
        if not self._session or not self._endpoint:
            return

        headers = {
            "Content-Type": "application/json",
            "DD-API-KEY": self.api_key or "",
        }
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self._session.post(
                    self._endpoint,
                    json=payload,
                    headers=headers,
                ) as response:
                    if response.status in {200, 202}:
                        return
                    elif response.status >= 500 and attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Datadog log failed with status {response.status}",
                            endpoint=self._endpoint.split("/v1/input/")[0] + "/v1/input/***",
                            status_code=response.status,
                            error_preview=error_text[:200] if error_text else "No error message",
                        )
                        return
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                logger.error("Datadog log request timed out after retries")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                logger.error(f"Failed to send log to Datadog: {str(e)}", exc_info=True)
                return

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None

    def __call__(self, message: Any) -> None:
        """Allow handler to be called directly."""
        self.sink(message)
