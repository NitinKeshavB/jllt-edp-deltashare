# DeltaShare API Layer

FastAPI backend service for the DeltaShare Enterprise Application. Enables data engineering teams to share Databricks assets (tables, views, streaming tables, materialized views, and notebooks) with internal and external clients through REST API endpoints.

**Package name:** `deltashare_api`
**Module name:** `dbrx_api` (note the discrepancy - be aware when importing)
**Python version:** 3.12+

## Quick Start

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

# Or manually with pip (from repo root)
pip install -e ".[dev]"

# Create .env file (see Configuration section)
# Run development server
make run-dev

# API available at http://localhost:8000
# Swagger docs at http://localhost:8000/
```

## Project Structure

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
│   │   ├── routes_share.py      # Share management endpoints
│   │   ├── routes_recipient.py  # Recipient management endpoints
│   │   ├── routes_catalog.py    # Unity Catalog endpoints
│   │   ├── routes_pipelines.py  # DLT pipeline management
│   │   ├── routes_schedule.py   # Job schedule management
│   │   ├── routes_metrics.py    # Pipeline metrics endpoints
│   │   └── routes_workflow.py   # Workflow provisioning endpoints
│   ├── dltshr/                  # Delta Sharing business logic
│   │   ├── share.py             # Share operations (Databricks SDK)
│   │   └── recipient.py         # Recipient operations (Databricks SDK)
│   ├── jobs/                    # Jobs and Pipelines management
│   │   ├── dbrx_pipelines.py    # DLT pipeline operations
│   │   ├── dbrx_schedule.py     # Job scheduling operations
│   │   └── dbrx_catalog.py      # Unity Catalog operations
│   ├── dbrx_auth/               # Databricks authentication
│   │   ├── token_gen.py         # Token generation
│   │   └── token_manager.py     # Token caching and management
│   ├── monitoring/              # Logging and observability
│   │   ├── logger.py            # Loguru configuration with sinks
│   │   ├── request_context.py   # Request context middleware
│   │   ├── azure_blob_handler.py    # Azure Blob Storage log handler
│   │   ├── postgresql_handler.py    # PostgreSQL log handler
│   │   └── datadog_handler.py       # Datadog log handler
│   ├── metrics/                 # Metrics collection
│   │   └── dbrx_job_metrics.py  # Job metrics extraction
│   └── workflow/                # Workflow provisioning system
│       ├── enums.py             # Status/type enums
│       ├── models/              # Pydantic models (share pack, tenant, etc.)
│       ├── db/                  # PostgreSQL SCD2 repositories (16 tables)
│       ├── orchestrator/        # Provisioning (NEW + UPDATE strategies)
│       ├── parsers/             # YAML + Excel share pack parsers
│       ├── queue/               # Azure Storage Queue client
│       └── validators/          # Strategy detection + validation
├── tests/                       # Test suite
│   ├── unit_tests/              # Unit tests (14 test files)
│   ├── fixtures/                # Reusable test fixtures (8 fixture files)
│   └── conftest.py              # Pytest configuration
├── version.txt                  # Version management (0.0.1)
├── Makefile                     # Development commands
├── run.sh                       # Task runner script
└── README.md                    # This file
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
| `dltshr_workspace_url` | `None` | Reference workspace URL (actual URL comes from `X-Workspace-URL` header) |

#### Azure Blob Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `enable_blob_logging` | `false` | Enable blob logging |
| `azure_storage_account_url` | `None` | Storage account URL |
| `azure_storage_sas_url` | `None` | SAS URL for blob container |
| `azure_storage_logs_container` | `deltashare-logs` | Container name |

#### PostgreSQL Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `enable_postgresql_logging` | `false` | Enable DB logging |
| `postgresql_connection_string` | `None` | Connection string |
| `postgresql_log_table` | `application_logs` | Table name |
| `postgresql_min_log_level` | `WARNING` | Min level: WARNING, ERROR, CRITICAL |

#### Datadog Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `enable_datadog_logging` | `false` | Enable Datadog logging |
| `dd_api_key` | `None` | Datadog API key |

#### Workflow System

| Variable | Default | Description |
|----------|---------|-------------|
| `enable_workflow` | `false` | Enable workflow provisioning system |
| `domain_db_connection_string` | `None` | PostgreSQL connection for workflow domain DB |
| `azure_queue_connection_string` | `None` | Azure Storage Queue connection (optional, for async processing) |
| `azure_queue_name` | `sharepack-processing` | Queue name for share pack provisioning |

### Local Development (.env file)

Create a `.env` file in `api_layer/`:

```env
# Databricks Authentication (Required)
client_id=your-service-principal-client-id
client_secret=your-service-principal-secret
account_id=your-databricks-account-id

# Blob Logging (Optional)
enable_blob_logging=false
azure_storage_account_url=
azure_storage_logs_container=deltashare-logs

# PostgreSQL Logging (Optional)
enable_postgresql_logging=false
postgresql_connection_string=

# Datadog Logging (Optional)
enable_datadog_logging=false
dd_api_key=

# Workflow System (Optional)
enable_workflow=false
domain_db_connection_string=
#azure_queue_connection_string=
```

### Azure Web App Deployment

Set variables in **Configuration > Application settings**. The app auto-detects Azure Web App and reads from App Settings.

## Make Commands

| Command | Description |
|---------|-------------|
| `make install` | Install all dev dependencies |
| `make run-dev` | Start development server (port 8000) |
| `make test` | Run tests with coverage report |
| `make test-quick` | Run tests without slow markers |
| `make lint` | Run all linters (black, isort, autoflake) |
| `make clean` | Remove build artifacts and cache |
| `make build` | Build distribution package |
| `make serve-coverage-report` | Serve HTML coverage report |

### OpenAPI Generation (Azure APIM)

| Command | Description |
|---------|-------------|
| `make generate-openapi` | Generate OpenAPI spec (default: dev) |
| `make generate-openapi-dev` | Generate for development environment |
| `make generate-openapi-uat` | Generate for UAT environment |
| `make generate-openapi-prod` | Generate for production environment |
| `make generate-openapi-all` | Generate for all environments |

## Testing

### Running Tests

```bash
# All tests with coverage
make test

# Quick tests (skip slow markers)
make test-quick

# Specific test file
bash run.sh run-tests tests/unit_tests/test_routes_share.py

# Specific test function
bash run.sh run-tests tests/unit_tests/test_routes_share.py::test_function_name

# Tests matching pattern
bash run.sh run-tests tests/ -k "test_create"
```

### Test Organization

```
tests/
├── unit_tests/
│   ├── test_routes_health.py       # Health endpoint tests
│   ├── test_routes_share.py        # Share routes tests
│   ├── test_routes_recipient.py    # Recipient routes tests
│   ├── test_routes_pipelines.py    # Pipeline routes tests
│   ├── test_routes_schedule.py     # Schedule routes tests
│   ├── test_dbrx_pipelines.py      # Pipeline SDK tests
│   ├── test_dltshr_share.py        # Share business logic tests
│   ├── test_dltshr_recipient.py    # Recipient business logic tests
│   ├── test_token_gen.py           # Token generation tests
│   ├── test_token_manager.py       # Token management tests
│   ├── test_dependencies.py        # Dependencies tests
│   ├── test_errors.py              # Error handling tests
│   ├── test_logging.py             # Logging system tests
│   └── test_data_pipelines.py      # Data pipeline tests
├── fixtures/                       # Reusable test fixtures
│   ├── app_fixtures.py             # FastAPI test client
│   ├── databricks_fixtures.py      # Databricks SDK mocks
│   ├── pipeline_fixtures.py        # Pipeline test data
│   ├── schedule_fixtures.py        # Schedule test data
│   ├── business_logic_fixtures.py  # Business logic fixtures
│   ├── azure_fixtures.py           # Azure service mocks
│   └── logging_fixtures.py         # Logging test fixtures
└── conftest.py                     # Pytest configuration
```

## API Endpoints

### Health Checks
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Basic health check |
| GET | `/health/ready` | Readiness check with dependencies |
| GET | `/health/logging/test` | Test blob storage logging |

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
| PUT | `/shares/{name}/recipients/remove` | Remove recipients from share |

### Recipient Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/recipients` | List all recipients |
| GET | `/recipients/{name}` | Get recipient details |
| POST | `/recipients/d2d/{name}` | Create D2D recipient (Databricks-to-Databricks) |
| POST | `/recipients/d2o/{name}` | Create D2O recipient (TOKEN-based) |
| DELETE | `/recipients/{name}` | Delete recipient |
| PUT | `/recipients/{name}/ipaddress/add` | Add IP addresses to allowlist |
| PUT | `/recipients/{name}/ipaddress/revoke` | Remove IP addresses from allowlist |
| POST | `/recipients/{name}/tokens/rotate` | Rotate recipient access token |
| PUT | `/recipients/{name}/description/update` | Update recipient description |
| PUT | `/recipients/{name}/expiration_time/update` | Update token expiration |

### Catalog Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/catalogs` | List Unity Catalog catalogs |
| GET | `/catalogs/{name}` | Get catalog details |

### Pipeline Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pipelines` | List all DLT pipelines |
| GET | `/pipelines/{name}` | Get pipeline details |
| GET | `/pipelines/{name}/configuration` | Get pipeline configuration |
| GET | `/pipelines/{name}/libraries` | Get pipeline libraries |
| POST | `/pipelines` | Create new DLT pipeline |
| PATCH | `/pipelines/{name}/continuous` | Update continuous mode |
| POST | `/pipelines/{name}/full-refresh` | Start full refresh |
| POST | `/pipelines/{name}/notifications/add` | Add notification emails |
| POST | `/pipelines/{name}/notifications/remove` | Remove notification emails |
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

### Metrics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pipelines/{pipeline_id}/metrics` | Get pipeline execution metrics |
| GET | `/pipelines/{name}/job-runs/metrics` | Get job run metrics by name |

### Workflow (Feature-Flagged: `enable_workflow=true`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/workflow/sharepack/upload_and_validate` | Upload and validate YAML/Excel share pack |
| GET | `/workflow/sharepack/{id}` | Get share pack status and details |
| GET | `/workflow/health` | Workflow system health check |

### Required Headers

All API requests (except health) must include:
```
X-Workspace-URL: https://<workspace>.azuredatabricks.net
Ocp-Apim-Subscription-Key: <your-subscription-key>
```

## Architecture

### Application Layers

1. **Routes Layer** (`routes/`)
   - FastAPI route handlers with request validation
   - Calls business logic from `dltshr/`, `jobs/`, and `workflow/` modules
   - Uses dependencies for workspace URL validation and subscription key verification

2. **Business Logic Layer** (`dltshr/` and `jobs/`)
   - `dltshr/share.py`: Share operations (create, delete, add/remove data objects, manage recipients)
   - `dltshr/recipient.py`: Recipient operations (create D2D/D2O, manage IPs, rotate tokens)
   - `jobs/dbrx_pipelines.py`: DLT pipeline management
   - `jobs/dbrx_schedule.py`: Job scheduling operations
   - `jobs/dbrx_catalog.py`: Unity Catalog operations
   - Authenticates via `dbrx_auth/token_manager.py` (thread-safe, in-memory caching)

3. **Workflow System** (`workflow/`) - Feature-flagged
   - Share Pack upload (YAML + Excel) with validation
   - SCD Type 2 data model across 16 PostgreSQL tables
   - NEW and UPDATE provisioning strategies
   - Azure Storage Queue for async processing (optional)

4. **Monitoring** (`monitoring/`)
   - Structured logging with `loguru`
   - Request context middleware (request ID, client IP, user identity)
   - Azure Blob Storage sink for persistent logs
   - PostgreSQL sink for critical logs (WARNING+)
   - Datadog sink for centralized observability

### Data Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Routes    │ --> │   Services   │ --> │  Databricks SDK │
│ (FastAPI)   │     │ (dltshr/jobs)│     │                 │
└─────────────┘     └──────────────┘     └─────────────────┘
       │                   │
       v                   v
┌─────────────┐     ┌──────────────┐
│  Schemas    │     │ Token Manager│
│ (Pydantic)  │     │   (Cached)   │
└─────────────┘     └──────────────┘
```

## Code Quality

```bash
# Run all linters
make lint

# Pre-commit hooks (auto-runs on commit)
pre-commit install
pre-commit run --all-files
```

### Style Guide
- **Line length:** 119 characters
- **Formatter:** black
- **Import sorting:** isort (VERTICAL_HANGING_INDENT profile)
- **Unused imports:** autoflake
- **Docstrings:** Google style
- **Type hints:** Required for all public functions

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Import errors | Run `make install` from `api_layer/` |
| `python-multipart` error | Run `pip install -e ".[dev]"` from repo root |
| App hangs on startup | Comment out `azure_queue_connection_string` in `.env` if not connected to Azure |
| Token expired | Delete `databricks_token` from `.env`, restart server |
| 502 Bad Gateway | Verify `X-Workspace-URL` header is correct |
| Permission denied | Check service principal has required Databricks permissions |
| Config not loading | Check `.env` file exists in `api_layer/` directory |
| Pre-commit isort fails | Ensure `.pre-commit-config.yaml` points to `./pyproject.toml` (not `./api_layer/pyproject.toml`) |

## Additional Resources

- [Main Project README](../README.md) - Project overview and quick start
- [CLAUDE.md](../.claude/CLAUDE.md) - Detailed development guidelines
- [API Documentation (Swagger)](http://localhost:8000/) - Interactive API docs (when running locally)
- [Databricks SDK Documentation](https://databricks-sdk-py.readthedocs.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Confluence](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/Enterprise+Delta+Share+Application)
