# Testing Quick Reference

## 📁 Test File Locations

All test files are now organized in `tests/unit_tests/`:

```
tests/
├── conftest.py                      # Shared fixtures & mocks
├── consts.py                        # Test constants
└── unit_tests/                      # All unit tests here
    ├── test_routes_share.py         # 41 Share API tests
    ├── test_routes_recipient.py     # 47 Recipient API tests
    └── test_logging.py              # Logging handler tests
```

## 🚀 Common Commands

### Run All Tests
```bash
# With coverage report
make test

# Without coverage (faster)
python -m pytest tests/unit_tests/ --no-cov

# Verbose output
python -m pytest tests/unit_tests/ -v
```

### Run Specific Test Files
```bash
# Share endpoint tests
python -m pytest tests/unit_tests/test_routes_share.py -v

# Recipient endpoint tests
python -m pytest tests/unit_tests/test_routes_recipient.py -v

# Logging tests
python -m pytest tests/unit_tests/test_logging.py -v
```

### Run Specific Test Classes
```bash
# Test share creation
python -m pytest tests/unit_tests/test_routes_share.py::TestCreateShare -v

# Test recipient deletion
python -m pytest tests/unit_tests/test_routes_recipient.py::TestDeleteRecipient -v

# Test IP management
python -m pytest tests/unit_tests/test_routes_recipient.py::TestAddClientIPToRecipient -v
```

### Run Specific Test Functions
```bash
python -m pytest tests/unit_tests/test_routes_share.py::TestGetShareByName::test_get_share_by_name_success -v
```

## 📊 Coverage Commands

### Generate HTML Coverage Report
```bash
# Run tests with HTML coverage
python -m pytest tests/unit_tests/ --cov=src/dbrx_api --cov-report=html

# Serve the report
make serve-coverage-report
# Open http://localhost:8000 in browser
```

### View Coverage in Terminal
```bash
python -m pytest tests/unit_tests/ --cov=src/dbrx_api --cov-report=term-missing
```

### Generate XML Coverage (for CI/CD)
```bash
python -m pytest tests/unit_tests/ --cov=src/dbrx_api --cov-report=xml
```

## 🎯 Running Tests by Pattern

### Run tests matching a pattern
```bash
# All tests with "create" in the name
python -m pytest tests/unit_tests/ -k "create" -v

# All tests with "delete" in the name
python -m pytest tests/unit_tests/ -k "delete" -v

# All success scenario tests
python -m pytest tests/unit_tests/ -k "success" -v

# All error scenario tests
python -m pytest tests/unit_tests/ -k "not_found or permission_denied" -v
```

### Run tests by marker
```bash
# Run only slow tests
python -m pytest tests/unit_tests/ -m "slow"

# Skip slow tests
python -m pytest tests/unit_tests/ -m "not slow"
```

## 🐛 Debugging Tests

### Show print statements
```bash
python -m pytest tests/unit_tests/ -s
```

### Stop on first failure
```bash
python -m pytest tests/unit_tests/ -x
```

### Show full traceback
```bash
python -m pytest tests/unit_tests/ --tb=long
```

### Show only failed tests
```bash
python -m pytest tests/unit_tests/ --lf
```

### Run only failed tests and new tests
```bash
python -m pytest tests/unit_tests/ --ff
```

## 📝 Test Statistics

- **Total Tests**: 87
- **Share Endpoint Tests**: 41
- **Recipient Endpoint Tests**: 47
- **Logging Tests**: ~13
- **Current Pass Rate**: ~69%

## 🎭 Available Fixtures

### Core Fixtures
- `client` - FastAPI TestClient
- `app` - FastAPI application instance
- `mock_settings` - Mocked Settings

### Mock Fixtures
- `mock_share_business_logic` - All share functions
- `mock_recipient_business_logic` - All recipient functions
- `mock_workspace_client` - Databricks SDK
- `mock_azure_blob_client` - Azure Blob Storage
- `mock_postgresql_pool` - PostgreSQL pool
- `mock_share_info` - ShareInfo factory
- `mock_recipient_info` - RecipientInfo factory

## 📋 Quick Examples

### Example: Run share tests with coverage
```bash
python -m pytest tests/unit_tests/test_routes_share.py --cov=src/dbrx_api/routes_share --cov-report=term-missing
```

### Example: Run specific failing test with full output
```bash
python -m pytest tests/unit_tests/test_routes_recipient.py::TestRotateRecipientToken::test_rotate_token_success -vv --tb=long -s
```

### Example: Run all tests and generate HTML report
```bash
python -m pytest tests/unit_tests/ --cov=src/dbrx_api --cov-report=html && open htmlcov/index.html
```

## 🔍 Finding Tests

### List all tests without running
```bash
python -m pytest tests/unit_tests/ --collect-only
```

### Count tests
```bash
python -m pytest tests/unit_tests/ --collect-only -q | wc -l
```

## ✅ CI/CD Integration

### GitHub Actions Example
```yaml
- name: Run tests
  run: python -m pytest tests/unit_tests/ --cov=src/dbrx_api --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## 📚 More Information

- Full documentation: `TEST_SUITE_SUMMARY.md`
- Fixture details: `tests/conftest.py`
- Test configuration: `pyproject.toml`
