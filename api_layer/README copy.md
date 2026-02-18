# DeltaShare Enterprise Application

Enterprise MVP application enabling data engineering teams to share Databricks assets (tables, views, streaming tables, materialized views, and notebooks) with internal and external clients.

## Overview

DeltaShare provides a REST API for managing Delta Sharing operations on Databricks workspaces. It supports sharing data with clients who may or may not have Databricks in their infrastructure.

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Backend** | Python 3.12+, FastAPI, Pydantic |
| **Databricks** | Delta Sharing API, DLT Pipelines, Jobs API, Unity Catalog |
| **Cloud** | Azure Web App, Azure Service Plan |
| **Authentication** | Azure AD / Entra ID (OAuth2) |
| **Logging** | Azure Blob Storage, PostgreSQL, Datadog |
| **Workflow** | PostgreSQL (SCD2), Azure Storage Queue |

## Project Structure

```
JLLT-EDP-DeltaShare/
├── .claude/                       # Claude AI documentation
├── .cursor/rules/                 # Cursor AI rules
├── .github/workflows/             # CI/CD pipelines
├── api_layer/                     # Backend API (FastAPI)
│   ├── src/dbrx_api/
│   │   ├── routes/                # API route handlers
│   │   │   ├── routes_health.py
│   │   │   ├── routes_share.py
│   │   │   ├── routes_recipient.py
│   │   │   ├── routes_catalog.py
│   │   │   ├── routes_pipelines.py
│   │   │   ├── routes_schedule.py
│   │   │   ├── routes_metrics.py
│   │   │   └── routes_workflow.py
│   │   ├── schemas/               # Pydantic models
│   │   ├── dltshr/                # Delta Sharing SDK
│   │   ├── jobs/                  # Databricks Jobs/Pipelines/Catalogs
│   │   ├── dbrx_auth/             # Databricks authentication
│   │   ├── monitoring/            # Logging (Blob, PostgreSQL, Datadog)
│   │   ├── metrics/               # Pipeline metrics collection
│   │   ├── workflow/              # Workflow provisioning system
│   │   │   ├── models/            # Pydantic models (share pack, tenant, etc.)
│   │   │   ├── db/                # PostgreSQL SCD2 repositories
│   │   │   ├── orchestrator/      # Provisioning (NEW + UPDATE strategies)
│   │   │   ├── parsers/           # YAML + Excel share pack parsers
│   │   │   ├── queue/             # Azure Storage Queue client
│   │   │   └── validators/        # Strategy detection + validation
│   │   ├── main.py                # FastAPI app factory
│   │   ├── settings.py            # Configuration
│   │   ├── dependencies.py        # FastAPI dependencies
│   │   └── errors.py              # Exception handlers
│   ├── tests/                     # Test suite (26 test files)
│   ├── Makefile                   # Development commands
│   └── run.sh                     # Task runner
├── pyproject.toml                 # Python project configuration
├── config.json                    # Project configuration
├── apilayer_config.json           # Environment-specific API config
└── databricks_config.json         # Databricks workspace config
```

## Quick Start

```bash
# Navigate to api_layer
cd api_layer

# Install dependencies
make install

# Create .env file with credentials (see api_layer/README.md)

# Run development server
make run-dev

# API available at http://localhost:8000
```

## API Endpoints

**Required Header:** `X-Workspace-URL: https://<workspace>.azuredatabricks.net`

### Health
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/health/ready` | Readiness check |

### Shares
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/shares` | List all shares |
| GET | `/shares/{name}` | Get share details |
| POST | `/shares` | Create new share |
| DELETE | `/shares/{name}` | Delete share |
| PUT | `/shares/{name}/dataobject/add` | Add tables/views |
| PUT | `/shares/{name}/dataobject/revoke` | Remove tables/views |
| PUT | `/shares/{name}/recipients/add` | Add recipients to share |
| PUT | `/shares/{name}/recipients/remove` | Remove recipients from share |

### Recipients
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/recipients` | List all recipients |
| GET | `/recipients/{name}` | Get recipient details |
| POST | `/recipients/d2d/{name}` | Create D2D recipient |
| POST | `/recipients/d2o/{name}` | Create D2O (TOKEN) recipient |
| DELETE | `/recipients/{name}` | Delete recipient |
| PUT | `/recipients/{name}/ipaddress/add` | Add IP addresses |
| PUT | `/recipients/{name}/ipaddress/revoke` | Remove IP addresses |
| POST | `/recipients/{name}/tokens/rotate` | Rotate access token |

### Catalogs
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/catalogs` | List Unity Catalog catalogs |
| GET | `/catalogs/{name}` | Get catalog details |

### Pipelines
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pipelines` | List all DLT pipelines |
| GET | `/pipelines/{name}` | Get pipeline details |
| POST | `/pipelines` | Create DLT pipeline |
| PATCH | `/pipelines/{name}/continuous` | Update continuous mode |
| POST | `/pipelines/{name}/full-refresh` | Full refresh |
| DELETE | `/pipelines/{pipeline_id}` | Delete pipeline |

### Schedules
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/schedules` | List all schedules (auto-paginated) |
| GET | `/schedules/pipeline/{name}` | List schedules for pipeline |
| POST | `/pipelines/{name}/schedules` | Create schedule |
| PATCH | `/pipelines/{name}/schedules/{job}/cron` | Update cron |
| PATCH | `/pipelines/{name}/schedules/{job}/timezone` | Update timezone |
| DELETE | `/pipelines/{name}/schedules/{job}` | Delete schedule |

### Metrics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/pipelines/{pipeline_id}/metrics` | Get pipeline execution metrics |
| GET | `/pipelines/{name}/job-runs/metrics` | Get job run metrics |

### Workflow (Feature-Flagged)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/workflow/sharepack/upload_and_validate` | Upload YAML/Excel share pack |
| GET | `/workflow/sharepack/{id}` | Get share pack status |
| GET | `/workflow/health` | Workflow system health check |

## Development

```bash
# Run tests with coverage
make test

# Run linters (black, isort, autoflake)
make lint

# Pre-commit hooks
pre-commit install
pre-commit run --all-files
```

## Environments

| Environment | Purpose | Azure Naming |
|-------------|---------|--------------|
| `dev` | Development | `*-dev-*` |
| `uat` | User acceptance | `*-uat-*` |
| `prd` | Production | `*-prd-*` |

## Documentation

- [API Layer README](api_layer/README.md) - Detailed setup and configuration
- [Confluence](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/Enterprise+Delta+Share+Application)
- [API Docs (Swagger)](http://localhost:8000/) - Local development
