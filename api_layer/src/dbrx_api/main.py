import os
from pathlib import Path
from textwrap import dedent
from typing import Any

import pydantic
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
from loguru import logger

from dbrx_api.dbrx_auth.token_manager import TokenManager
from dbrx_api.errors import DATABRICKS_SDK_AVAILABLE
from dbrx_api.errors import DatabricksError
from dbrx_api.errors import handle_broad_exceptions
from dbrx_api.errors import handle_databricks_errors
from dbrx_api.errors import handle_pydantic_validation_errors
from dbrx_api.monitoring.logger import configure_logger
from dbrx_api.monitoring.request_context import RequestContextMiddleware
from dbrx_api.routes.routes_catalog import ROUTER_CATALOG
from dbrx_api.routes.routes_health import ROUTER_HEALTH
from dbrx_api.routes.routes_metrics import ROUTER_DBRX_METRICS
from dbrx_api.routes.routes_pipelines import ROUTER_DBRX_PIPELINES
from dbrx_api.routes.routes_recipient import ROUTER_RECIPIENT
from dbrx_api.routes.routes_schedule import ROUTER_DBRX_SCHEDULE
from dbrx_api.routes.routes_share import ROUTER_SHARE
from dbrx_api.settings import Settings


def _detect_environment() -> str:
    """Detect if running in Azure Web App or locally."""
    # Azure Web App sets WEBSITE_INSTANCE_ID
    if os.getenv("WEBSITE_INSTANCE_ID"):
        return "azure-web-app"
    # Check if .env file exists (local development)
    elif Path(".env").exists():
        return "local-env-file"
    else:
        return "local-env-vars"


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create a FastAPI application.

    Configuration is loaded directly from environment variables via pydantic-settings.
    - Azure Web App: Set variables as App Settings (Configuration > Application settings)
    - Local development: Use a .env file in the api_layer directory
    """
    # Initialize settings from environment variables
    settings = settings or Settings()

    # Configure logger with Azure/PostgreSQL/Datadog logging if enabled in settings
    configure_logger(
        dd_service=settings.dd_service,
        enable_blob_logging=settings.enable_blob_logging,
        azure_storage_url=settings.azure_storage_account_url,
        azure_storage_sas_url=settings.azure_storage_sas_url,
        blob_container=settings.azure_storage_logs_container,
        enable_postgresql_logging=settings.enable_postgresql_logging,
        postgresql_connection_string=settings.postgresql_connection_string,
        postgresql_table=settings.postgresql_log_table,
        postgresql_min_level=settings.postgresql_min_log_level,
        enable_datadog_logging=settings.enable_datadog_logging,
        dd_api_key=settings.dd_api_key,
        dd_env=os.getenv("ENVIRONMENT"),
    )

    # Log configuration after logger is configured (so it appears in Datadog)
    logger.info(
        "Configuration loaded successfully",
        client_id_set=bool(settings.client_id),
        account_id_set=bool(settings.account_id),
        blob_logging=settings.enable_blob_logging,
        postgresql_logging=settings.enable_postgresql_logging,
        datadog_logging=settings.enable_datadog_logging and bool(settings.dd_api_key),
        datadog_api_key_set=bool(settings.dd_api_key),
    )

    # Log startup (workspace URL is per-request via X-Workspace-URL header)
    logger.info(
        "Starting DeltaShare API application",
        workspace_url_mode="per-request-header",
        reference_url=settings.dltshr_workspace_url,
    )

    app = FastAPI(
        title="Delta Share API",
        version="v1",
        description=dedent(
            """
        ![Maintained by](https://img.shields.io/badge/Maintained_by-EDP%20Delta%20share_Team-green?style=for-the-badge)


        | Helpful Links | Notes |
        | --- | --- |
        | [Delta Share Confluence ](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/Enterprise+Delta+Share+Application) |`update-in-progress` |
        | [Delta Share Dev Team](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20587905070/Delta+Share+team) |`update-in-progress` |
        | [Delta Share CDR Sign off](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/jlltknowledgebase/pages/20324713069/External+Delta+Sharing+Framework+-+Architectural+Design+High+Level) | `signed-off` |
        | [Delta Share Project Repo](https://github.com/JLLT-Apps/JLLT-EDP-DeltaShare) | `Databricks-API-Web repo` |
        | [Delta Share status codes](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/edit-v2/20587249733?draftShareId=a715edeb-f8fc-4c02-90c4-a40ffdff3ecd) | `update-in-progress` |
        | [API Status](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20587970637/API+Dev+Status) | <img alt="Static Badge" src="https://img.shields.io/badge/Recipient_Done-Green?style=for-the-badge&logoColor=green"> <img alt="Static Badge" src="https://img.shields.io/badge/share_Done-blue?style=for-the-badge&color=blue"> |
        """
        ),
        docs_url=None,  # Disable default docs to use custom
        generate_unique_id_function=custom_generate_unique_id,
        swagger_ui_parameters={
            "defaultModelsExpandDepth": -1,  # Hide schemas section
            "defaultModelExpandDepth": 1,  # Keep models collapsed if shown
        },
    )
    app.state.settings = settings

    # Initialize TokenManager for in-memory token caching (thread-safe)
    # This eliminates the need for environment variable caching
    app.state.token_manager = TokenManager(
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        account_id=settings.account_id,
    )
    logger.info("TokenManager initialized for in-memory token caching")

    # Custom Swagger UI with smaller example text
    @app.get("/", include_in_schema=False)
    async def custom_swagger_ui_html():
        return HTMLResponse(
            content=f"""
            <!DOCTYPE html>
            <html>
            <head>
                <link type="text/css" rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
                <title>{app.title} - Swagger UI</title>
                <style>
                    /* Make example text smaller and italicized to match description */
                    .parameter__example,
                    .parameter__example .example,
                    .parameter__example .example__value {{
                        font-size: 12px !important;
                        font-style: italic !important;
                    }}

                    /* Hide Swagger logo */
                    .topbar-wrapper img,
                    .topbar-wrapper .link {{
                        display: none !important;
                    }}

                    /* Hide filter/search box and Explore text */
                    .filter-container,
                    .operation-filter-input,
                    input[placeholder="Filter by tag"],
                    .topbar-wrapper input,
                    .wrapper .link,
                    .topbar .wrapper section {{
                        display: none !important;
                    }}

                    /* Hide any element containing "Explore" text */
                    .topbar span,
                    .topbar label {{
                        display: none !important;
                    }}

                    /* Download button styling */
                    .download-openapi-btn {{
                        position: fixed;
                        top: 10px;
                        right: 20px;
                        z-index: 10000;
                        background-color: #4990e2;
                        color: white;
                        padding: 10px 20px;
                        border-radius: 4px;
                        text-decoration: none;
                        font-weight: 500;
                        font-size: 14px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
                        transition: background-color 0.2s;
                    }}

                    .download-openapi-btn:hover {{
                        background-color: #357abd;
                    }}
                </style>
            </head>
            <body>
                <a href="{app.openapi_url}" download="openapi.json" class="download-openapi-btn">Download OpenAPI JSON</a>
                <div id="swagger-ui"></div>
                <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
                <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
                <script>
                    const ui = SwaggerUIBundle({{
                        url: '{app.openapi_url}',
                        dom_id: '#swagger-ui',
                        presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIStandalonePreset
                        ],
                        layout: "StandaloneLayout",
                        deepLinking: true,
                        defaultModelsExpandDepth: -1,
                        defaultModelExpandDepth: 1,
                        filter: false
                    }})

                    // Pre-fill X-Workspace-URL header input with example value
                    setTimeout(() => {{
                        const observer = new MutationObserver(() => {{
                            const workspaceUrlInput = document.querySelector('input[placeholder*="X-Workspace-URL"], input[data-param-name="X-Workspace-URL"]');
                            if (workspaceUrlInput && !workspaceUrlInput.value) {{
                                workspaceUrlInput.value = 'https://adb-1234567890123456.12.azuredatabricks.net';
                                workspaceUrlInput.placeholder = 'https://adb-1234567890123456.12.azuredatabricks.net';
                            }}
                        }});
                        observer.observe(document.body, {{ childList: true, subtree: true }});
                    }}, 1000);
                </script>
            </body>
            </html>
            """
        )

    # Add request context middleware for tracking who/where requests come from
    app.add_middleware(RequestContextMiddleware)
    app.include_router(ROUTER_HEALTH, prefix="/api")
    app.include_router(ROUTER_CATALOG, prefix="/api")
    app.include_router(ROUTER_RECIPIENT, prefix="/api")
    app.include_router(ROUTER_SHARE, prefix="/api")
    app.include_router(ROUTER_DBRX_PIPELINES, prefix="/api")
    app.include_router(ROUTER_DBRX_SCHEDULE, prefix="/api")
    app.include_router(ROUTER_DBRX_METRICS, prefix="/api")

    # Workflow system integration (feature-flagged)
    if settings.enable_workflow and settings.domain_db_connection_string:
        logger.info("Initializing workflow system")

        from dbrx_api.routes.routes_workflow import ROUTER_WORKFLOW
        from dbrx_api.workflow.db.pool import DomainDBPool
        from dbrx_api.workflow.queue.queue_client import SharePackQueueClient

        # Initialize domain database pool
        domain_db_pool = DomainDBPool(settings.domain_db_connection_string)
        app.state.domain_db_pool = domain_db_pool

        # Initialize queue client if configured
        if settings.azure_queue_connection_string:
            queue_client = SharePackQueueClient(settings.azure_queue_connection_string, settings.azure_queue_name)
            app.state.queue_client = queue_client
            logger.info("Workflow queue client initialized")
        else:
            logger.warning("Azure queue not configured - workflow upload will work but processing won't")

        # Register workflow router
        app.include_router(ROUTER_WORKFLOW, prefix="/api")

        logger.success("Workflow system enabled", queue_enabled=bool(settings.azure_queue_connection_string))

        # Add startup/shutdown hooks for workflow
        @app.on_event("startup")
        async def startup_workflow():
            """Initialize workflow database and start queue consumer."""
            if hasattr(app.state, "domain_db_pool"):
                await app.state.domain_db_pool.initialize()
                logger.success("Workflow database initialized")

                # Start queue consumer (provisioning only)
                if hasattr(app.state, "queue_client"):
                    import asyncio

                    from dbrx_api.workflow.queue.queue_consumer import start_queue_consumer

                    app.state.queue_consumer_task = asyncio.create_task(
                        start_queue_consumer(app.state.queue_client, app.state.domain_db_pool)
                    )
                    logger.success("Share pack queue consumer started")

        @app.on_event("shutdown")
        async def shutdown_workflow():
            """Close workflow database connections."""
            if hasattr(app.state, "domain_db_pool"):
                await app.state.domain_db_pool.close()
                logger.info("Workflow database closed")

    else:
        logger.info("Workflow system disabled (enable_workflow=false or domain_db_connection_string not set)")

    app.add_exception_handler(
        exc_class_or_status_code=pydantic.ValidationError,
        handler=handle_pydantic_validation_errors,
    )

    # Add Databricks error handler if SDK is available
    if DATABRICKS_SDK_AVAILABLE:
        app.add_exception_handler(
            exc_class_or_status_code=DatabricksError,
            handler=handle_databricks_errors,
        )

    app.middleware("http")(handle_broad_exceptions)

    # Override OpenAPI schema generation to produce 3.0.3 compatible spec
    app.openapi = lambda: custom_openapi_schema(app)

    return app


def custom_generate_unique_id(route: APIRoute):
    """
    Generate prettier `operationId`s in the OpenAPI schema.

    These become the function names in generated client SDKs.
    """
    if route.tags:
        return f"{route.tags[0]}-{route.name}"
    return route.name


def custom_openapi_schema(app: FastAPI) -> dict[str, Any]:
    """
    Generate OpenAPI 3.0.3 compatible schema for Azure API Management.

    Azure API Management only supports OpenAPI 3.0.x, not 3.1.0.
    This function converts FastAPI's default 3.1.0 schema to 3.0.3.
    """
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Convert from OpenAPI 3.1.0 to 3.0.3 for Azure API Management compatibility
    openapi_schema["openapi"] = "3.0.3"

    # Remove 3.1.0-only fields from info object
    if "summary" in openapi_schema.get("info", {}):
        del openapi_schema["info"]["summary"]

    # Convert nullable fields from 3.1.0 format to 3.0.x format
    # In 3.1.0: "type": ["string", "null"]
    # In 3.0.x: "type": "string", "nullable": true
    def convert_schema_to_3_0(schema: dict[str, Any]) -> None:
        """Recursively convert schema from 3.1.0 to 3.0.3 format."""
        if not isinstance(schema, dict):
            return

        # Convert anyOf with null to nullable (common in Pydantic v2 output)
        # Example: anyOf: [{type: "string"}, {type: "null"}] -> type: "string", nullable: true
        if "anyOf" in schema and isinstance(schema["anyOf"], list):
            null_schema = None
            non_null_schemas = []

            for sub_schema in schema["anyOf"]:
                if isinstance(sub_schema, dict) and sub_schema.get("type") == "null":
                    null_schema = sub_schema
                else:
                    non_null_schemas.append(sub_schema)

            # If we have exactly one non-null schema and a null schema, simplify it
            if null_schema and len(non_null_schemas) == 1:
                non_null = non_null_schemas[0]
                # Merge the non-null schema into the parent
                for key, value in non_null.items():
                    schema[key] = value
                schema["nullable"] = True
                del schema["anyOf"]
            elif not null_schema:
                # No null type, recursively process
                for sub_schema in schema["anyOf"]:
                    convert_schema_to_3_0(sub_schema)

        # Convert type array with null to nullable
        if "type" in schema and isinstance(schema["type"], list):
            if "null" in schema["type"]:
                non_null_types = [t for t in schema["type"] if t != "null"]
                if len(non_null_types) == 1:
                    schema["type"] = non_null_types[0]
                    schema["nullable"] = True
                elif len(non_null_types) > 1:
                    # Multiple non-null types - use anyOf
                    schema["anyOf"] = [{"type": t} for t in non_null_types]
                    schema["nullable"] = True
                    del schema["type"]

        # Remove 3.1.0 specific keywords
        if "examples" in schema:
            # In 3.0.x, use example (singular) instead of examples (plural)
            if isinstance(schema["examples"], list) and len(schema["examples"]) > 0:
                schema["example"] = schema["examples"][0]
            del schema["examples"]

        # Recursively process nested schemas
        for key in ["properties", "items", "additionalProperties", "oneOf", "allOf"]:
            if key in schema:
                if key == "properties" and isinstance(schema[key], dict):
                    for prop_schema in schema[key].values():
                        convert_schema_to_3_0(prop_schema)
                elif key in ["oneOf", "allOf"] and isinstance(schema[key], list):
                    for sub_schema in schema[key]:
                        convert_schema_to_3_0(sub_schema)
                elif key in ["items", "additionalProperties"]:
                    convert_schema_to_3_0(schema[key])

    # Convert all schemas in components
    if "components" in openapi_schema and "schemas" in openapi_schema["components"]:
        for schema in openapi_schema["components"]["schemas"].values():
            convert_schema_to_3_0(schema)

    # Convert schemas in paths
    if "paths" in openapi_schema:
        for path_item in openapi_schema["paths"].values():
            if isinstance(path_item, dict):
                for operation in path_item.values():
                    if isinstance(operation, dict):
                        # Convert request body schemas
                        if "requestBody" in operation:
                            content = operation["requestBody"].get("content", {})
                            for media_type in content.values():
                                if "schema" in media_type:
                                    convert_schema_to_3_0(media_type["schema"])

                        # Convert response schemas
                        if "responses" in operation:
                            for response in operation["responses"].values():
                                if isinstance(response, dict) and "content" in response:
                                    for media_type in response["content"].values():
                                        if "schema" in media_type:
                                            convert_schema_to_3_0(media_type["schema"])

                        # Convert parameter schemas
                        if "parameters" in operation:
                            for param in operation["parameters"]:
                                if "schema" in param:
                                    convert_schema_to_3_0(param["schema"])

    app.openapi_schema = openapi_schema
    return app.openapi_schema


if __name__ == "__main__":
    import uvicorn

    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000)
