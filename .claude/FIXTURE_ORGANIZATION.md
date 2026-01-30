# Fixture Organization Guide

## Overview

All test fixtures are now organized in the `tests/fixtures/` directory, categorized by functionality for better maintainability and discoverability.

## Directory Structure

```
tests/
├── conftest.py                         # Minimal - just registers fixture modules
└── fixtures/                           # All fixtures organized here
    ├── app_fixtures.py                 # FastAPI app & settings
    ├── databricks_fixtures.py          # Databricks SDK mocks
    ├── azure_fixtures.py               # Azure service mocks
    ├── logging_fixtures.py             # Logging mocks
    ├── business_logic_fixtures.py      # Business logic mocks
    └── example_fixture.py              # Example (can be removed)
```

## Fixture Files

### 1. `app_fixtures.py` - Core Application Fixtures

**Purpose**: FastAPI application setup and configuration

**Fixtures**:
- `mock_settings` - Mocked Settings with test environment variables
  - Sets up test workspace URL, credentials
  - Disables blob and PostgreSQL logging by default

- `app` - FastAPI test application instance
  - Creates app with mocked settings
  - Includes all routes and middleware

- `client` - FastAPI TestClient
  - Used to make HTTP requests in tests
  - Automatically handles startup/shutdown

**Usage Example**:
```python
def test_endpoint(client):
    response = client.get("/shares/test_share")
    assert response.status_code == 200
```

---

### 2. `databricks_fixtures.py` - Databricks SDK Mocks

**Purpose**: Mock Databricks SDK components

**Fixtures**:
- `mock_auth_token` - Mocks authentication token generation
  - Returns a test token that expires in the future

- `mock_share_info` - Factory to create ShareInfo objects
  - Customizable name, owner, comment, timestamps
  - Usage: `share = mock_share_info(name="custom_share")`

- `mock_recipient_info` - Factory to create RecipientInfo objects
  - Supports D2D and D2O recipient types
  - Customizable authentication type, tokens, IP lists
  - Usage: `recipient = mock_recipient_info(name="test", auth_type=AuthenticationType.TOKEN)`

- `mock_workspace_client` - Complete Databricks SDK mock
  - Mocks shares API (list, get, create, delete, update)
  - Mocks recipients API (list, get, create, delete, update, rotate_token)
  - Mocks permissions API (get_share_permissions, update_share_permissions)

**Usage Example**:
```python
def test_with_custom_share(mock_share_info, mock_share_business_logic):
    custom_share = mock_share_info(name="my_share", owner="me")
    mock_share_business_logic["get"].return_value = custom_share
    # ... test logic
```

---

### 3. `azure_fixtures.py` - Azure Service Mocks

**Purpose**: Mock Azure Blob Storage and PostgreSQL

**Fixtures**:
- `mock_azure_blob_client` - Mocked BlobServiceClient
  - Mocks blob upload operations
  - Mocks container and blob client creation

- `mock_postgresql_pool` - Mocked asyncpg pool
  - Mocks async database connections
  - Mocks query execution

**Usage Example**:
```python
def test_azure_logging(mock_azure_blob_client):
    # Azure operations are automatically mocked
    pass
```

---

### 4. `logging_fixtures.py` - Logging Mocks

**Purpose**: Mock logging infrastructure

**Fixtures**:
- `mock_logger` - Mocked loguru logger
  - Returns dict with 'share' and 'recipient' loggers
  - Useful for verifying log calls

- `mock_azure_blob_handler` - Mocked Azure Blob log handler
  - Prevents actual blob writes during tests

- `mock_postgresql_handler` - Mocked PostgreSQL log handler
  - Prevents actual database writes during tests

**Usage Example**:
```python
def test_logging(client, mock_logger):
    client.get("/shares/test")
    assert mock_logger["share"].info.called
```

---

### 5. `business_logic_fixtures.py` - Business Logic Mocks

**Purpose**: Mock business logic functions to isolate route testing

**Fixtures**:
- `mock_share_business_logic` - Mocks all share functions
  - Returns dict with keys: list, get, create, delete, add_objects, revoke_objects, add_recipients, remove_recipients
  - Each value is a MagicMock that can be configured

- `mock_recipient_business_logic` - Mocks all recipient functions
  - Returns dict with keys: list, get, create_d2d, create_d2o, delete, rotate, add_ip, revoke_ip, update_desc, update_exp
  - Each value is a MagicMock that can be configured

**Usage Example**:
```python
def test_share_creation(client, mock_share_business_logic):
    # Mock the get to return None (share doesn't exist)
    mock_share_business_logic["get"].return_value = None

    response = client.post("/shares/new_share", params={"description": "Test"})

    assert response.status_code == 201
    mock_share_business_logic["create"].assert_called_once()
```

## How Fixtures Are Registered

The `conftest.py` file registers all fixture modules using `pytest_plugins`:

```python
pytest_plugins = [
    "tests.fixtures.app_fixtures",
    "tests.fixtures.databricks_fixtures",
    "tests.fixtures.azure_fixtures",
    "tests.fixtures.logging_fixtures",
    "tests.fixtures.business_logic_fixtures",
]
```

This allows pytest to automatically discover and make available all fixtures from these modules.

## Adding New Fixtures

To add a new fixture:

1. **Choose the appropriate category** or create a new fixture file
2. **Define the fixture** with `@pytest.fixture` decorator
3. **Register the module** in `conftest.py` if it's a new file
4. **Document the fixture** in this file

Example:
```python
# In tests/fixtures/app_fixtures.py
@pytest.fixture
def new_fixture():
    """Description of what this fixture provides."""
    return "test_value"
```

## Fixture Scopes

Most fixtures use the default `function` scope (created per test). Some fixtures that could benefit from different scopes:

- `session` - Created once per test session (expensive setup)
- `module` - Created once per test module
- `class` - Created once per test class
- `function` - Created for each test (default)

Example:
```python
@pytest.fixture(scope="session")
def expensive_fixture():
    # Setup code runs once for entire session
    return expensive_resource
```

## Best Practices

1. **Use factory fixtures** for creating test data with variations
   - Example: `mock_share_info()` can create different shares

2. **Keep fixtures focused** - One fixture should do one thing

3. **Document fixtures** - Include docstrings explaining purpose and usage

4. **Organize by category** - Keep related fixtures in the same file

5. **Use meaningful names** - Prefix mocks with `mock_`

6. **Avoid fixture interdependencies** where possible
   - If needed, document the dependency chain

## Common Patterns

### Pattern 1: Testing Success Scenarios
```python
def test_success(client, mock_share_business_logic):
    # Setup: configure mocks for success
    mock_share_business_logic["get"].return_value = mock_share_info()

    # Execute: call the endpoint
    response = client.get("/shares/test_share")

    # Verify: check response and mock calls
    assert response.status_code == 200
    mock_share_business_logic["get"].assert_called_once()
```

### Pattern 2: Testing Error Scenarios
```python
def test_not_found(client, mock_share_business_logic):
    # Setup: configure mocks to return None
    mock_share_business_logic["get"].return_value = None

    # Execute
    response = client.get("/shares/nonexistent")

    # Verify
    assert response.status_code == 404
```

### Pattern 3: Using Factory Fixtures
```python
def test_with_custom_data(client, mock_recipient_info, mock_recipient_business_logic):
    # Create custom test data
    custom_recipient = mock_recipient_info(
        name="custom",
        auth_type=AuthenticationType.DATABRICKS
    )

    # Configure mock to return it
    mock_recipient_business_logic["get"].return_value = custom_recipient

    # Test with custom data
    response = client.get("/recipients/custom")
    assert response.json()["authentication_type"] == "DATABRICKS"
```

## Troubleshooting

### Issue: Fixture not found
**Solution**: Ensure the fixture file is registered in `conftest.py` pytest_plugins

### Issue: Import errors in fixture files
**Solution**: Check that PYTHONPATH is set correctly in conftest.py

### Issue: Fixtures not cleaning up
**Solution**: Use `yield` in fixtures for proper cleanup:
```python
@pytest.fixture
def resource():
    r = setup_resource()
    yield r
    r.cleanup()  # Runs after test
```

### Issue: Mock not being applied
**Solution**: Ensure you're patching where the function is used, not where it's defined:
```python
# ✅ Correct - patch where it's imported
patch("dbrx_api.routes_share.get_shares")

# ❌ Wrong - patching the original module
patch("dbrx_api.dltshr.share.get_shares")
```

## Summary

The fixture organization provides:

✅ **Clear categorization** - Easy to find what you need
✅ **Better maintainability** - Related fixtures grouped together
✅ **Reusability** - Fixtures available across all tests
✅ **Documentation** - Each file focuses on one area
✅ **Scalability** - Easy to add new fixtures in appropriate categories

All fixtures are automatically discovered by pytest and available in your tests without explicit imports!
