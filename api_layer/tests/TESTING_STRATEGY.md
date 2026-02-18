# Testing Strategy

## Overview

This document defines the testing approach for the DeltaShare API. All test code lives under `api_layer/tests/`. No production code in `src/` is modified to satisfy tests.

## Layers

- **Unit (route handlers):** Test FastAPI route handlers with mocked dependencies (Databricks SDK, Azure, DB). Use `API_BASE` for all HTTP paths so tests match production (`/api/...`).
- **Unit (SDK / business logic):** Test `dbrx_api.jobs`, `dbrx_api.dltshr`, and other modules with mocked external I/O.
- **Integration (optional):** Mark with `@pytest.mark.slow`; no real external services in default runs. Document in this file if added.

## Principles

- **One test file per route module:** `test_routes_health.py`, `test_routes_share.py`, `test_routes_catalog.py`, etc.
- **Single API base path:** Use the `API_BASE` constant from `tests.consts` for every route request (e.g. `f"{API_BASE}/health"`).
- **Mock external I/O:** Databricks, Azure Storage, PostgreSQL, and queues are mocked. No network or real DB in unit tests.
- **Success and error paths:** Each public route has at least one success test and one error or validation test where applicable.
- **No changes to `src/`:** All changes for test redesign are confined to the `tests/` directory.

## Coverage Goals

- Every public route has at least one success and one error (or validation) test.
- Aim for high coverage of route handlers and key branches; use `pytest --cov` to track.

## Naming

- **Files:** `test_<module_or_feature>.py` (e.g. `test_routes_health.py`, `test_dbrx_pipelines.py`).
- **Classes:** `Test<Feature>` (e.g. `TestHealthEndpointsNoAuthRequired`).
- **Methods:** `test_<scenario>__<expected_outcome>` or `test_<scenario>` (e.g. `test_health_check_no_auth_required`, `test_list_shares_success`).

Align with project testing standards in `.cursor/rules/testing-standards.mdc`.

## Fixtures

- **Location:** `tests/fixtures/`; register plugins in `tests/conftest.py`.
- **API base:** Import `API_BASE` from `tests.consts` in route tests.
- **App/client:** Use `app`, `client`, `unauthenticated_client` from `tests.fixtures.app_fixtures`; add domain-specific fixtures (catalog, metrics, workflow) as needed.

## Markers

- Use `@pytest.mark.slow` for any integration-style tests that hit real services or are long-running.
- Document new markers in `pyproject.toml` and here if introduced.

## Running Tests

```bash
cd api_layer
make test
# or
pytest tests/ -v
pytest tests/ --cov=dbrx_api --cov-report=term-missing
```

## Lint / Format

Run formatters and linters on the tests folder as per project rules (e.g. `black tests/`, `isort tests/`, `flake8 tests/`).
