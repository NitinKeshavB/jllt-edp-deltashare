#!/usr/bin/env python3
"""
Generate OpenAPI JSON specification with Azure API Management (APIM) compatibility.

This script generates an OpenAPI 3.0.3 specification from the FastAPI app
that is compatible with Azure API Management requirements:
- Removes internal endpoints that shouldn't be exposed publicly
- Adds proper security schemes for Azure APIM
- Includes subscription key requirements
- Formats response schemas appropriately
- Adds server information for different environments

Usage:
    python apim_openapi/generate_openapi.py
    python apim_openapi/generate_openapi.py --output custom_path.json
    python apim_openapi/generate_openapi.py --env prod
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Add src to path for imports
script_dir = Path(__file__).parent.absolute()
project_root = script_dir.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    from dbrx_api.main import create_app
    from dbrx_api.settings import Settings
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running this script from the api_layer directory")
    print("and that all dependencies are installed (run 'make install')")
    sys.exit(1)


def get_environment_servers(env: str = "dev") -> list[dict[str, str]]:
    """Get server configurations for different environments."""
    servers = {
        "dev": [
            {
                "url": "https://your-apim-dev.azure-api.net/deltashare",
                "description": "Development environment (Azure APIM)",
            },
            {"url": "http://localhost:8000", "description": "Local development server"},
        ],
        "uat": [
            {"url": "https://your-apim-uat.azure-api.net/deltashare", "description": "UAT environment (Azure APIM)"}
        ],
        "prod": [
            {
                "url": "https://your-apim-prod.azure-api.net/deltashare",
                "description": "Production environment (Azure APIM)",
            }
        ],
    }
    return servers.get(env, servers["dev"])


def add_azure_apim_security(openapi_spec: dict[str, Any]) -> dict[str, Any]:
    """Add Azure APIM compatible security schemes."""
    # Add security schemes
    openapi_spec.setdefault("components", {}).setdefault("securitySchemes", {}).update(
        {
            "apimSubscriptionKey": {
                "type": "apiKey",
                "name": "Ocp-Apim-Subscription-Key",
                "in": "header",
                "description": "Azure API Management subscription key",
            },
            "workspaceUrl": {
                "type": "apiKey",
                "name": "X-Workspace-URL",
                "in": "header",
                "description": "Databricks workspace URL (e.g., https://adb-xxx.azuredatabricks.net)",
            },
        }
    )

    # Apply security requirements to all paths
    for path, methods in openapi_spec.get("paths", {}).items():
        for method, operation in methods.items():
            if method.lower() in ["get", "post", "put", "patch", "delete"]:
                # Skip health endpoints from requiring subscription key (for monitoring)
                if path.startswith("/health"):
                    operation["security"] = [{"workspaceUrl": []}]
                else:
                    operation["security"] = [{"apimSubscriptionKey": [], "workspaceUrl": []}]

    return openapi_spec


def filter_internal_endpoints(openapi_spec: dict[str, Any]) -> dict[str, Any]:
    """Remove internal endpoints that shouldn't be exposed in Azure APIM."""
    paths_to_remove = [
        "/",  # Custom Swagger UI
        "/docs",  # FastAPI docs
        "/redoc",  # ReDoc
        "/openapi.json",  # OpenAPI spec itself
    ]

    # Remove internal paths
    for path in paths_to_remove:
        openapi_spec.get("paths", {}).pop(path, None)

    return openapi_spec


def enhance_api_metadata(openapi_spec: dict[str, Any], env: str = "dev") -> dict[str, Any]:
    """Enhance API metadata for Azure APIM."""
    # Update API info
    info = openapi_spec.get("info", {})
    info.update(
        {
            "title": "DeltaShare Enterprise API",
            "description": """
# DeltaShare Enterprise API

Enterprise API for Databricks Delta Sharing operations. Enables data engineering teams to share Databricks assets (tables, views, streaming tables, materialized views, and notebooks) with internal and external clients.

## Features

- **Share Management**: Create and manage Delta shares with data objects
- **Recipient Management**: Handle D2D (Databricks-to-Databricks) and D2O (Databricks-to-Open) recipients
- **Pipeline Management**: DLT pipeline operations and monitoring
- **Schedule Management**: Job scheduling and automation
- **Metrics**: Pipeline execution metrics and monitoring

## Authentication

All requests require:
1. **Azure APIM Subscription Key**: `Ocp-Apim-Subscription-Key` header
2. **Databricks Workspace URL**: `X-Workspace-URL` header

## Environments

- **Development**: Testing and development
- **UAT**: User acceptance testing
- **Production**: Live environment

## Core Concepts

- **Share**: A named collection of Databricks data objects that can be shared with recipients
- **Recipient**: An entity (internal or external) that receives access to shared data
  - **D2D**: Recipients with Databricks infrastructure using DATABRICKS authentication
  - **D2O**: Recipients without Databricks using TOKEN authentication
- **Data Objects**: Tables, views, streaming tables, materialized views that can be added to shares

## Rate Limits

API calls are subject to Azure APIM rate limiting policies.

## Support

For API support and documentation, refer to the project documentation.
        """.strip(),
            "version": _get_api_version(),
            "contact": {
                "name": "Data Engineering Team",
                "url": "https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/Enterprise+Delta+Share+Application",
            },
            "license": {"name": "Proprietary", "url": "https://github.com/JLLT-Apps/JLLT-EDP-DeltaShare"},
        }
    )

    # Add servers for different environments
    openapi_spec["servers"] = get_environment_servers(env)

    # Add tags for better organization in Azure APIM
    openapi_spec["tags"] = [
        {"name": "Health", "description": "Health check and monitoring endpoints"},
        {"name": "Shares", "description": "Delta Share management operations"},
        {"name": "Recipients", "description": "Recipient management (D2D and D2O)"},
        {"name": "Pipelines", "description": "DLT pipeline management and operations"},
        {"name": "Schedules", "description": "Job scheduling and automation"},
        {"name": "Metrics", "description": "Pipeline metrics and monitoring"},
    ]

    return openapi_spec


def _get_api_version() -> str:
    """Get API version from version.txt file."""
    try:
        version_file = Path(__file__).parent.parent / "version.txt"
        if version_file.exists():
            return version_file.read_text().strip()
    except Exception:
        pass
    return "1.0.0"


def clean_openapi_spec(openapi_spec: dict[str, Any]) -> dict[str, Any]:
    """Clean up the OpenAPI spec for better Azure APIM compatibility."""
    # Ensure OpenAPI version is 3.0.3 (preferred by Azure APIM)
    openapi_spec["openapi"] = "3.0.3"

    # Clean up operation IDs to be more Azure APIM friendly
    for path, methods in openapi_spec.get("paths", {}).items():
        for method, operation in methods.items():
            if method.lower() in ["get", "post", "put", "patch", "delete"]:
                # Ensure operation ID is present and follows naming conventions
                if "operationId" not in operation:
                    # Generate operation ID from path and method
                    clean_path = path.replace("/", "_").replace("{", "").replace("}", "").strip("_")
                    operation["operationId"] = f"{method.lower()}_{clean_path}"

                # Ensure all operations have tags
                if "tags" not in operation:
                    # Assign tags based on path
                    if path.startswith("/health"):
                        operation["tags"] = ["Health"]
                    elif path.startswith("/shares"):
                        operation["tags"] = ["Shares"]
                    elif path.startswith("/recipients"):
                        operation["tags"] = ["Recipients"]
                    elif path.startswith("/pipelines"):
                        operation["tags"] = ["Pipelines"]
                    elif path.startswith("/schedules"):
                        operation["tags"] = ["Schedules"]
                    elif "metrics" in path:
                        operation["tags"] = ["Metrics"]
                    else:
                        operation["tags"] = ["General"]

    # Remove any null values that might cause issues
    openapi_spec = _remove_null_values(openapi_spec)

    return openapi_spec


def _remove_null_values(obj: Any) -> Any:
    """Recursively remove null values from the OpenAPI spec."""
    if isinstance(obj, dict):
        return {k: _remove_null_values(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_remove_null_values(item) for item in obj if item is not None]
    return obj


def generate_openapi_spec(env: str = "dev", skip_auth: bool = False) -> dict[str, Any]:
    """Generate the complete OpenAPI specification."""
    print(f"🔄 Generating OpenAPI spec for environment: {env}")

    # Load settings from environment variables or .env file
    # This ensures we use real credentials for proper app initialization
    if skip_auth:
        print("⚠️  Using minimal credentials for testing (skip-auth-validation enabled)")
        settings = Settings(
            client_id="test-client-id", client_secret="test-client-secret", account_id="test-account-id"
        )
    else:
        try:
            settings = Settings()
            print("✅ Loaded credentials from environment/configuration")
        except Exception as e:
            print(f"❌ Failed to load credentials: {e}")
            print("Make sure you have set the required environment variables:")
            print("  - CLIENT_ID (or client_id)")
            print("  - CLIENT_SECRET (or client_secret)")
            print("  - ACCOUNT_ID (or account_id)")
            print("Or create a .env file with these values for local development")
            print("Alternatively, use --skip-auth-validation for testing (not recommended for production)")
            raise

    # Create FastAPI app
    app = create_app(settings)

    # Generate base OpenAPI spec
    openapi_spec = app.openapi()

    # Apply all transformations
    openapi_spec = filter_internal_endpoints(openapi_spec)
    openapi_spec = enhance_api_metadata(openapi_spec, env)
    openapi_spec = add_azure_apim_security(openapi_spec)
    openapi_spec = clean_openapi_spec(openapi_spec)

    print(f"✅ Generated OpenAPI spec with {len(openapi_spec.get('paths', {}))} endpoints")

    return openapi_spec


def main() -> None:
    """Main script entry point."""
    parser = argparse.ArgumentParser(description="Generate OpenAPI JSON for Azure APIM")
    parser.add_argument(
        "--output",
        "-o",
        default="apim_openapi/openapi.json",
        help="Output file path (default: apim_openapi/openapi.json)",
    )
    parser.add_argument(
        "--env",
        "-e",
        choices=["dev", "uat", "prod"],
        default="dev",
        help="Environment to generate spec for (default: dev)",
    )
    parser.add_argument("--pretty", "-p", action="store_true", help="Pretty print JSON output")
    parser.add_argument(
        "--skip-auth-validation",
        action="store_true",
        help="Skip authentication validation (for testing - uses minimal dummy credentials)",
    )

    args = parser.parse_args()

    try:
        # Generate OpenAPI spec
        openapi_spec = generate_openapi_spec(args.env, args.skip_auth_validation)

        # Write to file
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            if args.pretty:
                json.dump(openapi_spec, f, indent=2, ensure_ascii=False)
            else:
                json.dump(openapi_spec, f, ensure_ascii=False)

        print(f"✅ OpenAPI spec written to: {output_path.absolute()}")
        print(f"📊 Environment: {args.env}")
        print(f"📝 Endpoints: {len(openapi_spec.get('paths', {}))}")
        print("🔧 Security: Azure APIM compatible")

        # Print some stats
        paths = openapi_spec.get("paths", {})
        method_count = sum(
            len([m for m in methods.keys() if m.lower() in ["get", "post", "put", "patch", "delete"]])
            for methods in paths.values()
        )
        print(f"🌐 Total operations: {method_count}")

        # Validation check
        required_fields = ["openapi", "info", "paths", "components"]
        missing_fields = [field for field in required_fields if field not in openapi_spec]
        if missing_fields:
            print(f"⚠️  Warning: Missing required fields: {missing_fields}")
        else:
            print("✅ OpenAPI spec validation: OK")

    except Exception as e:
        print(f"❌ Error generating OpenAPI spec: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
