# GitHub Actions CI Fix Guide

## Issue
The CI test failed with:
```
RuntimeError: The starlette.testclient module requires the httpx package to be installed.
```

## Root Cause
When testing the installed wheel in CI, the test dependencies (`httpx` and `pytest-asyncio`) were not being installed.

## ✅ Fixes Applied

### 1. Updated `run.sh` (Line 84)

**Before:**
```bash
pip install ./dist/*.whl pytest pytest-cov
```

**After:**
```bash
pip install "./dist/*.whl[test]"
```

This now installs the wheel with the `[test]` extras, which automatically includes all test dependencies from `pyproject.toml`.

### 2. Verified `pyproject.toml` (Line 35)

The test dependencies are already correct:
```python
test = ["pytest", "pytest-cov", "httpx", "pytest-asyncio"]
```

## How to Update GitHub Actions Workflow

If you have a separate test workflow (not shown in `main_agenticops.yml`), update it to install test dependencies:

### Option 1: Install with test extras (Recommended)
```yaml
- name: Install package and test dependencies
  run: |
    python -m pip install --upgrade pip
    pip install build
    python -m build
    pip install "./dist/*.whl[test]"
```

### Option 2: Install test dependencies manually
```yaml
- name: Install package and test dependencies
  run: |
    python -m pip install --upgrade pip
    pip install build
    python -m build
    pip install ./dist/*.whl
    pip install pytest pytest-cov httpx pytest-asyncio
```

### Option 3: Install from source with extras (Simplest for CI)
```yaml
- name: Install package with test dependencies
  run: |
    python -m pip install --upgrade pip
    pip install ".[test]"
```

## Testing Locally

Test the wheel installation locally:
```bash
bash run.sh test:wheel-locally
```

This will:
1. Create a fresh virtual environment
2. Build the wheel
3. Install wheel with `[test]` extras
4. Run tests against the installed package
5. Clean up

## Complete Test Workflow Example

Here's a complete GitHub Actions workflow for testing:

```yaml
name: Run Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.12"]

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v5
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install ".[test,dbrx,api,azure]"

    - name: Run tests
      run: |
        pytest tests/ \
          --cov=src/dbrx_api \
          --cov-report=xml \
          --cov-report=term-missing \
          --junit-xml=test-reports/report.xml
      env:
        # Mock environment variables for testing
        DLTSHR_WORKSPACE_URL: https://test.azuredatabricks.net/
        CLIENT_ID: test-client-id
        CLIENT_SECRET: test-client-secret
        ACCOUNT_ID: test-account-id
        ENABLE_BLOB_LOGGING: false
        ENABLE_POSTGRESQL_LOGGING: false

    - name: Upload coverage reports
      uses: codecov/codecov-action@v3
      if: matrix.python-version == '3.12'
      with:
        file: ./coverage.xml
        fail_ci_if_error: false
```

## Verification Checklist

- [x] `pyproject.toml` has `httpx` and `pytest-asyncio` in test dependencies
- [x] `run.sh` installs wheel with `[test]` extras
- [ ] GitHub Actions workflow installs test dependencies (if you have a test workflow)
- [ ] Local test run passes: `bash run.sh test:wheel-locally`
- [ ] CI test run passes

## Next Steps

1. **Commit the fix:**
   ```bash
   git add run.sh
   git commit -m "fix: install test dependencies in CI"
   git push
   ```

2. **Add test workflow (if needed):**
   - Create `.github/workflows/test.yml` with the example above
   - Or add a test job to your existing workflow

3. **Verify CI passes:**
   - Check GitHub Actions tab
   - Ensure tests pass

## Required Environment Variables for CI

If you add a test workflow, set these as repository secrets or in the workflow:

```yaml
env:
  DLTSHR_WORKSPACE_URL: ${{ secrets.DLTSHR_WORKSPACE_URL }}
  CLIENT_ID: ${{ secrets.CLIENT_ID }}
  CLIENT_SECRET: ${{ secrets.CLIENT_SECRET }}
  ACCOUNT_ID: ${{ secrets.ACCOUNT_ID }}
```

Or use mock values for testing (as shown in the example above).

## Summary

✅ **Fixed:** `run.sh` now installs wheel with `[test]` extras
✅ **Verified:** `pyproject.toml` has all required test dependencies
⏭️ **Next:** Update GitHub Actions workflow (if you have one) to install test dependencies

The fix ensures that whenever the wheel is tested (locally or in CI), all test dependencies including `httpx` and `pytest-asyncio` are automatically installed.
