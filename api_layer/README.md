# DeltaShare API Layer

FastAPI backend service for the DeltaShare Enterprise Application. Enables data engineering teams to share Databricks assets (tables, views, streaming tables, materialized views, and notebooks) with internal and external clients through REST API endpoints.

**Package name:** `deltashare_api`  
**Module name:** `dbrx_api` (note the discrepancy - be aware when importing)  
**Python version:** 3.12+

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- Azure Service Principal with Databricks access
- Databricks workspace with Delta Sharing enabled

### Installation

```bash
# Navigate to api_layer
cd api_layer

# Install dev dependencies
make install

# Or manually with pip
pip install -e .[dev]

# Create .env file (see Configuration section)
# Run development server
make run-dev

# API available at http://localhost:8000
# Swagger docs at http://localhost:8000/
```

## 📁 Project Structure

```
api_layer/
├── src/dbrx_api/
│   ├── main.py                  # FastAPI app creation and configuration
│   ├── settings.py              # Pydantic settings (reads from env vars)
│   ├── dependencies.py          # FastAPI dependencies (workspace URL validation, auth)
│   ├── errors.py                # Global error handlers
│   ├── schemas/
│   │   └── schemas.py           # Request/response models
│   ├── routes/                  # FastAPI route handlers
│   │   ├── routes_health.py     # Health check endpoints
│   │   ├── routes_share.py      # Share-related API endpoints
│   │   ├── routes_recipient.py  # Recipient-related API endpoints
│   │   ├── routes_pipelines.py  # Pipeline management endpoints
│   │   ├── routes_schedule.py   # Schedule management endpoints
│   │   └── routes_metrics.py    # Metrics endpoints
│   ├── dltshr/                  # Delta Sharing business logic
│   │   ├── share.py             # Share operations (Databricks SDK calls)
│   │   └── recipient.py         # Recipient operations (Databricks SDK calls)
│   ├── jobs/                    # Jobs and Pipelines management
│   │   ├── dbrx_pipelines.py    # DLT pipeline operations
│   │   └── dbrx_schedule.py     # Job scheduling operations
│   ├── dbrx_auth/               # Databricks authentication
│   │   ├── token_gen.py         # Token generation
│   │   └── token_manager.py     # Token caching and management
│   ├── monitoring/              # Logging and observability
│   │   ├── logger.py            # Loguru configuration with sinks
│   │   ├── request_context.py   # Request context middleware
│   │   ├── azure_blob_handler.py    # Azure Blob Storage log handler
│   │   └── postgresql_handler.py    # PostgreSQL log handler
│   └── metrics/                 # Metrics and monitoring
│       └── dbrx_job_metrics.py  # Job metrics collection
├── tests/                       # Test suite (327+ tests)
│   ├── unit_tests/              # All unit tests
│   ├── fixtures/                # Reusable test fixtures
│   └── conftest.py              # Pytest configuration
├── version.txt                  # Version management
├── pyproject.toml              # Python project configuration
├── Makefile                    # Development commands
└── README.md                   # This file
```

## Configuration

### Environment Detection

The app automatically detects where to load configuration from:

| Environment | Detection | Config Source |
|-------------|-----------|---------------|
| **Azure Web App** | `WEBSITE_INSTANCE_ID` exists | App Settings |
| **Local (with .env)** | `.env` file exists | `.env` file |
| **Local (no .env)** | Neither | Shell environment variables |

### Environment Variables

All variables are **lowercase** and **case-insensitive**:

#### Required (Databricks Auth)

| Variable | Description |
|----------|-------------|
| `client_id` | Azure Service Principal Client ID |
| `client_secret` | Azure Service Principal Secret |
| `account_id` | Databricks Account ID |

#### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `dltshr_workspace_url` | `None` | Reference workspace URL |
| `databricks_token` | `None` | Cached OAuth token (auto-managed) |
| `token_expires_at_utc` | `None` | Token expiry (auto-managed) |

#### Azure Blob Logging (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `enable_blob_logging` | `false` | Enable blob logging |
| `azure_storage_account_url` | `None` | Storage account URL |
| `azure_storage_logs_container` | `deltashare-logs` | Container name |

#### PostgreSQL Logging (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `enable_postgresql_logging` | `false` | Enable DB logging |
| `postgresql_connection_string` | `None` | Connection string |
| `postgresql_log_table` | `application_logs` | Table name |
| `postgresql_min_log_level` | `WARNING` | Min level: WARNING, ERROR, CRITICAL |

### Local Development (.env file)

Create a `.env` file in `api_layer/`:

```env
# Databricks Authentication (Required)
client_id=your-service-principal-client-id
client_secret=your-service-principal-secret
account_id=your-databricks-account-id

# Optional
dltshr_workspace_url=https://adb-xxx.azuredatabricks.net

# Blob Logging (Optional)
enable_blob_logging=false
azure_storage_account_url=
azure_storage_logs_container=deltashare-logs

# PostgreSQL Logging (Optional)
enable_postgresql_logging=false
postgresql_connection_string=
postgresql_log_table=application_logs
postgresql_min_log_level=WARNING
```

### Azure Web App Deployment

Set these in **Configuration > Application settings**:

```
client_id = <your-client-id>
client_secret = <your-secret>
account_id = <your-account-id>
```

The app will automatically detect Azure Web App and read from App Settings.

## Make Commands

| Command | Description |
|---------|-------------|
| `make install` | Install all dev dependencies |
| `make run-dev` | Start development server (port 8000) |
| `make test` | Run tests with coverage report |
| `make test-quick` | Run tests without coverage |
| `make lint` | Run all linters (black, isort, flake8, pylint, mypy) |
| `make clean` | Remove build artifacts and cache |
| `make build` | Build distribution package |
| `make serve-coverage-report` | Serve HTML coverage report |

### OpenAPI Generation (Azure APIM)

| Command | Description |
|---------|-------------|
| `make generate-openapi` | Generate OpenAPI spec for development (default) |
| `make generate-openapi-dev` | Generate OpenAPI spec for development environment |
| `make generate-openapi-uat` | Generate OpenAPI spec for UAT environment |
| `make generate-openapi-prod` | Generate OpenAPI spec for production environment |
| `make generate-openapi-all` | Generate OpenAPI specs for all environments |

**Note**: The generation script is located in `scripts/generate_openapi.py` and outputs JSON files to the `apim_openapi/` directory. See [`scripts/README.md`](scripts/README.md) for detailed documentation.

## 🧪 Testing

The project includes comprehensive test coverage with **327+ unit tests**.

### Test Organization
```
tests/
├── unit_tests/              # All unit tests
│   ├── test_routes_*.py     # API endpoint tests
│   ├── test_dltshr_*.py     # Business logic tests
│   ├── test_dbrx_*.py       # Databricks integration tests
│   └── test_*.py            # Other component tests
├── fixtures/                # Reusable test fixtures
│   ├── app_fixtures.py      # FastAPI test client
│   ├── databricks_fixtures.py # Databricks SDK mocks
│   ├── pipeline_fixtures.py # Pipeline test data
│   └── schedule_fixtures.py # Schedule test data
└── conftest.py              # Pytest configuration
```

### Running Tests

```bash
# All tests with coverage
make test

# All tests without coverage (faster)
make test-quick

# Run with correct PYTHONPATH
PYTHONPATH=src python -m pytest

# Specific test file
PYTHONPATH=src python -m pytest tests/unit_tests/test_routes_share.py -v

# Specific test function
PYTHONPATH=src python -m pytest tests/unit_tests/test_routes_share.py::TestCreateShare::test_create_share_success

# Tests matching pattern
PYTHONPATH=src python -m pytest -k "test_create" -v

# With coverage report
PYTHONPATH=src python -m pytest --cov=src/dbrx_api

# Serve HTML coverage report
make serve-coverage-report
```

### Test Configuration

- **Line length**: 119 characters (black, flake8, isort)
- **Coverage**: Configured but not enforced minimum
- **Async support**: Using pytest-asyncio for async tests
- **Mocking**: Extensive use of unittest.mock for Databricks SDK

## 📡 API Endpoints

### Health Checks
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Basic health check |
| GET | `/health/ready` | Readiness check with dependencies |

### Share Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/shares` | List all shares |
| GET | `/shares/{name}` | Get share details |
| POST | `/shares` | Create new share |
| DELETE | `/shares/{name}` | Delete share |
| PUT | `/shares/{name}/dataobject/add` | Add tables/views to share |
| PUT | `/shares/{name}/dataobject/revoke` | Remove tables/views from share |
| PUT | `/shares/{name}/recipients/add` | Add recipients to share |
| PUT | `/shares/{name}/recipients/revoke` | Remove recipients from share |

### Recipient Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/recipients` | List all recipients |
| GET | `/recipients/{name}` | Get recipient details |
| POST | `/recipients/d2d/{name}` | Create D2D recipient (Databricks-to-Databricks) |
| POST | `/recipients/d2o/{name}` | Create D2O recipient (TOKEN-based) |
| DELETE | `/recipients/{name}` | Delete recipient |
| PUT | `/recipients/{name}/ip/add` | Add IP addresses to recipient allowlist |
| PUT | `/recipients/{name}/ip/revoke` | Remove IP addresses from allowlist |
| POST | `/recipients/{name}/rotate-token` | Rotate recipient access token |

### Pipeline Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pipelines` | List all DLT pipelines |
| GET | `/pipelines/{name}` | Get pipeline details |
| POST | `/pipelines` | Create new DLT pipeline |
| PATCH | `/pipelines/{name}/continuous` | Update continuous mode |
| POST | `/pipelines/{name}/full-refresh` | Start full refresh |
| DELETE | `/pipelines/{pipeline_id}` | Delete pipeline |

### Schedule Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/schedules` | List all schedules (auto-paginated) |
| GET | `/schedules/pipeline/{name}` | List schedules for specific pipeline |
| POST | `/pipelines/{name}/schedules` | Create schedule for pipeline |
| PATCH | `/pipelines/{name}/schedules/{job}/cron` | Update cron expression |
| PATCH | `/pipelines/{name}/schedules/{job}/timezone` | Update timezone |
| DELETE | `/pipelines/{name}/schedules/{job}` | Delete specific schedule |
| DELETE | `/pipelines/{name}/schedules` | Delete all schedules for pipeline |

### Metrics Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pipelines/{pipeline_id}/metrics` | Get pipeline execution metrics |

### Required Headers

All API requests must include:
```
X-Workspace-URL: https://<workspace>.azuredatabricks.net
Ocp-Apim-Subscription-Key: <your-subscription-key>
```

## Code Quality

```bash
# Run all linters
make lint

# Individual tools
black src/                    # Format code
isort src/                    # Sort imports
flake8 src/                   # Lint code
pylint src/                   # Static analysis
mypy src/                     # Type checking

# Pre-commit hooks (auto-runs on commit)
pre-commit install
pre-commit run --all-files
```

### Style Guide
- **Line length:** 119 characters
- **Imports:** Single line, alphabetically sorted
- **Docstrings:** Google style
- **Type hints:** Required for all public functions

## 🏗️ Architecture

### Application Layers

1. **Routes Layer** (`routes_*.py`)
   - FastAPI route handlers
   - Request validation and response serialization
   - Calls business logic functions from `dltshr/` and `jobs/` modules
   - Uses dependencies for workspace URL validation and subscription key verification

2. **Business Logic Layer** (`dltshr/` and `jobs/`)
   - `dltshr/share.py`: Share operations (create, delete, add/remove data objects, manage recipients)
   - `dltshr/recipient.py`: Recipient operations (create D2D/D2O recipients, manage IPs, rotate tokens)
   - `jobs/dbrx_pipelines.py`: DLT pipeline management (create, update, delete, full refresh)
   - `jobs/dbrx_schedule.py`: Job scheduling operations (create, update, delete schedules)
   - Uses Databricks SDK (`databricks.sdk.WorkspaceClient`)
   - Authenticates via `dbrx_auth.token_gen.get_auth_token()`

3. **Configuration & Dependencies**
   - `Settings` class uses `pydantic_settings` to load from environment variables
   - Settings are attached to FastAPI app state: `request.app.state.settings`
   - **IMPORTANT**: `dltshr_workspace_url` in Settings is deprecated - use `X-Workspace-URL` header instead

4. **Monitoring & Logging**
   - Structured logging with `loguru`
   - Request context middleware captures: request ID, client IP, user identity, user agent, request path
   - Optional Azure Blob Storage sink for persistent logs
   - Optional PostgreSQL sink for critical logs (WARNING and above)

### Key Patterns

- **Per-Request Workspace URLs**: Each API request includes `X-Workspace-URL` header specifying the Databricks workspace
- **Authentication Layers**: Azure API Management + FastAPI dependency validation + Databricks OAuth2
- **Token Management**: OAuth tokens cached with 5-minute refresh buffer
- **Error Handling**: Global middleware with specific Databricks SDK error mapping
- **Response Naming**: PascalCase for API responses, snake_case for requests

### Data Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Routes    │ ──▶ │   Services   │ ──▶ │  Databricks SDK │
│ (FastAPI)   │     │ (dltshr/jobs)│     │                 │
└─────────────┘     └──────────────┘     └─────────────────┘
       │                   │
       ▼                   ▼
┌─────────────┐     ┌──────────────┐
│  Schemas    │     │ Token Manager│
│ (Pydantic)  │     │   (Cached)   │
└─────────────┘     └──────────────┘
```

## 🐛 Troubleshooting

### Common Issues

1. **Import Error: No module named 'dbrx_api'**
   ```bash
   # Ensure PYTHONPATH is set correctly
   export PYTHONPATH=src
   ```

2. **Authentication Failures**
   - Check service principal credentials in `.env`
   - Verify Databricks account ID
   - Ensure workspace URL in `X-Workspace-URL` header is correct

3. **Test Failures**
   ```bash
   # Run with proper PYTHONPATH
   PYTHONPATH=src python -m pytest
   ```

4. **Token Parsing Errors**
   - Check network connectivity to Databricks
   - Verify service principal has proper permissions
   - Check if workspace URL is reachable

5. **Query Parameter Issues**
   - For schedules endpoint, use `pipeline_name_search_string` not `pipeline_name`
   - Example: `/schedules?pipeline_name_search_string=my-pipeline`

### Issues & Solutions Table

| Issue | Solution |
|-------|----------|
| Import errors | Run `make install` to install dependencies |
| Token expired | Delete `databricks_token` from .env, restart server |
| 502 Bad Gateway | Verify `X-Workspace-URL` header is correct |
| Permission denied | Check service principal has required Databricks permissions |
| Config not loading | Check `.env` file exists in `api_layer/` directory |
| Tests failing | Ensure `PYTHONPATH=src` when running pytest |

## 📚 Additional Resources

- [Main Project README](../README.md) - Project overview and quick start
- [CLAUDE.md](../.claude/CLAUDE.md) - Detailed development guidelines
- [API Documentation (Swagger)](http://localhost:8000/) - Interactive API docs (when running locally)
- [Databricks SDK Documentation](https://databricks-sdk-py.readthedocs.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Confluence Documentation](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/Enterprise+Delta+Share+Application)

### Getting Help

- Check logs for detailed error messages
- Review API documentation at `/` endpoint
- Examine test files for usage examples
- Refer to CLAUDE.md for development patterns
