"""
DLT Pipeline Tests - README
============================

This directory contains comprehensive test suites for the DLT Pipeline API functionality.

## Test Structure

```
tests/
├── fixtures/
│   ├── pipeline_fixtures.py      # Pipeline-specific fixtures
│   ├── databricks_fixtures.py    # Databricks SDK mocks
│   └── app_fixtures.py            # App and client fixtures
├── unit_tests/
│   ├── test_dbrx_pipelines.py    # SDK function tests
│   └── test_routes_pipelines.py  # API endpoint tests
├── test_data_pipelines.py         # Test data and constants
└── conftest.py                    # Pytest configuration

## Running Tests

### Run All Pipeline Tests
```bash
cd api_layer
pytest tests/unit_tests/test_dbrx_pipelines.py tests/unit_tests/test_routes_pipelines.py -v
```

### Run SDK Tests Only
```bash
pytest tests/unit_tests/test_dbrx_pipelines.py -v
```

### Run Route Tests Only
```bash
pytest tests/unit_tests/test_routes_pipelines.py -v
```

### Run Specific Test Class
```bash
pytest tests/unit_tests/test_dbrx_pipelines.py::TestUpdatePipelineContinuous -v
```

### Run Specific Test
```bash
pytest tests/unit_tests/test_routes_pipelines.py::TestPipelineFullRefreshEndpoint::test_full_refresh_success -v
```

### Run with Coverage
```bash
pytest tests/unit_tests/test_dbrx_pipelines.py tests/unit_tests/test_routes_pipelines.py --cov=src/dbrx_api --cov-report=html
```

### Run with Detailed Output
```bash
pytest tests/unit_tests/test_dbrx_pipelines.py -vv -s
```

## Test Coverage

### SDK Functions (`test_dbrx_pipelines.py`)

**TestGetPipelineByName:**
- ✅ Successful retrieval by name
- ✅ Pipeline not found
- ✅ Exception handling

**TestUpdatePipelineContinuous:**
- ✅ Update to continuous mode (True)
- ✅ Update to triggered mode (False)
- ✅ Pipeline not found error
- ✅ Settings preservation (config, catalog, target, notifications, tags)

**TestPipelineFullRefresh:**
- ✅ Full refresh on idle pipeline (immediate start)
- ✅ Full refresh on running pipeline (stop then start)
- ✅ Timeout when pipeline doesn't stop
- ✅ Pipeline not found error

**TestDeletePipeline:**
- ✅ Successful deletion
- ✅ Permission denied error

### API Endpoints (`test_routes_pipelines.py`)

**TestPipelineAuthenticationHeaders:**
- ✅ Missing workspace URL header rejection

**TestUpdatePipelineContinuousEndpoint:**
- ✅ Update to continuous mode (True)
- ✅ Update to triggered mode (False)
- ✅ Pipeline not found (404)
- ✅ Permission denied (403)
- ✅ Missing required field (422)
- ✅ Invalid data type (422)

**TestPipelineFullRefreshEndpoint:**
- ✅ Successful full refresh
- ✅ Pipeline not found (404)
- ✅ Timeout (408)
- ✅ Permission denied (403)
- ✅ Generic error (400)

**TestDeletePipelineEndpoint:**
- ✅ Successful deletion
- ✅ Pipeline not found (404)
- ✅ Permission denied (403)

**TestGetPipelineByNameEndpoint:**
- ✅ Successful retrieval
- ✅ Pipeline not found (404)

**TestListPipelinesEndpoint:**
- ✅ List all pipelines
- ✅ Empty pipeline list

## Test Fixtures

### Pipeline Fixtures (`pipeline_fixtures.py`)

**Mock Objects:**
- `mock_pipeline_state_info` - Lightweight pipeline info
- `mock_pipeline_spec` - Pipeline specification
- `mock_get_pipeline_response` - Full pipeline details
- `mock_create_pipeline_response` - Pipeline creation response
- `mock_start_update_response` - Update start response
- `mock_pipeline_notifications` - Notifications config
- `mock_pipeline_cluster` - Cluster with tags
- `mock_pipelines_api` - Mocked Pipelines API
- `mock_workspace_client_pipelines` - Full workspace client mock

**Sample Data:**
- `sample_pipeline_config` - Valid configuration dict
- `sample_create_pipeline_request` - Complete create request
- `sample_update_continuous_request` - Continuous mode update
- `sample_pipeline_tags` - Tag examples

## Test Data (`test_data_pipelines.py`)

**Constants:**
- `VALID_PIPELINE_NAMES` - Valid pipeline name examples
- `INVALID_PIPELINE_NAMES` - Invalid name examples
- `VALID_CONFIGURATIONS` - Valid config dictionaries
- `INVALID_CONFIGURATIONS` - Invalid config examples
- `VALID_NOTIFICATION_LISTS` - Valid email/AD group lists
- `INVALID_NOTIFICATION_LISTS` - Invalid notification examples
- `VALID_TAGS` - Valid tag dictionaries
- `INVALID_TAGS` - Invalid tag examples
- `VALID_LIBRARY_PATHS` - Valid library paths
- `INVALID_LIBRARY_PATHS` - Invalid library paths

**Scenarios:**
- `FULL_REFRESH_SCENARIOS` - Different pipeline states for refresh
- `CONTINUOUS_MODE_SCENARIOS` - Continuous mode transitions
- `ERROR_MESSAGES` - Expected error message patterns
- `SAMPLE_PIPELINES` - Complete pipeline definitions

## Writing New Tests

### Test Naming Convention
```python
def test_{action}_{condition}_{expected_result}(self, ...):
    """Test description."""
```

Examples:
- `test_update_continuous_to_true_success`
- `test_full_refresh_pipeline_not_found`
- `test_delete_pipeline_permission_denied`

### Test Structure
```python
def test_example(self, client, mock_auth_token, fixture):
    """Test description."""
    # Setup
    # ... prepare mocks and data

    # Execute
    result = function_under_test(params)

    # Assert
    assert result == expected
    mock.assert_called_once()
```

### Using Fixtures
```python
def test_with_fixtures(
    self,
    client,                          # FastAPI test client
    mock_auth_token,                 # Mocked auth
    mock_pipeline_state_info,        # Pipeline info factory
    mock_get_pipeline_response,      # Full pipeline factory
    sample_pipeline_config,          # Sample config dict
):
    # Create test data using factories
    pipeline = mock_pipeline_state_info(pipeline_name="test")
    full_pipeline = mock_get_pipeline_response(configuration=sample_pipeline_config)
```

## Common Patterns

### Mocking SDK Functions
```python
with patch("dbrx_api.jobs.dbrx_pipelines.function_name") as mock_func:
    mock_func.return_value = expected_value
    # ... test code
```

### Testing HTTP Responses
```python
response = client.put("/endpoint", json=payload)
assert response.status_code == status.HTTP_200_OK
data = response.json()
assert data["field"] == expected_value
```

### Testing Error Handling
```python
mock_func.return_value = "Error message"
response = client.post("/endpoint")
assert response.status_code == status.HTTP_404_NOT_FOUND
assert "error" in response.json()["detail"].lower()
```

## Troubleshooting

### Import Errors
If you see import errors, ensure you're running from the `api_layer` directory:
```bash
cd api_layer
pytest tests/...
```

### Fixture Not Found
Make sure `conftest.py` includes `pipeline_fixtures` in `pytest_plugins`.

### Mock Not Working
Ensure the patch path matches the import location in the code being tested:
```python
# Correct - patches where it's used
@patch("dbrx_api.routes_pipelines.function")

# Incorrect - patches where it's defined
@patch("dbrx_api.jobs.dbrx_pipelines.function")
```

## Best Practices

1. **Test One Thing** - Each test should verify one behavior
2. **Descriptive Names** - Test names should describe what they test
3. **AAA Pattern** - Arrange (setup), Act (execute), Assert (verify)
4. **Independent Tests** - Tests should not depend on each other
5. **Use Fixtures** - Reuse setup code via fixtures
6. **Mock External Calls** - Don't call real Databricks APIs
7. **Test Edge Cases** - Not just happy paths
8. **Keep Tests Fast** - Mock time.sleep(), don't make real delays

## CI/CD Integration

These tests are designed to run in CI/CD pipelines:

```yaml
- name: Run Pipeline Tests
  run: |
    cd api_layer
    pytest tests/unit_tests/test_dbrx_pipelines.py tests/unit_tests/test_routes_pipelines.py \
      --cov=src/dbrx_api \
      --cov-report=xml \
      --junitxml=test-results.xml
```

## Coverage Goals

Target coverage for pipeline code:
- **SDK Functions**: 90%+ coverage
- **API Routes**: 85%+ coverage
- **Edge Cases**: All error paths tested
- **Happy Paths**: All success scenarios tested
"""
