# Claude Code Documentation

This folder contains comprehensive documentation for the Delta Share API project.

## 📚 Documentation Index

### Core Documentation
- **[CLAUDE.md](./CLAUDE.md)** - Main project overview and development guide
  - Project structure
  - Development commands
  - Architecture patterns
  - Databricks SDK usage

- **[HEALTH_CHECK_GUIDE.md](./HEALTH_CHECK_GUIDE.md)** - Health check endpoints documentation
  - Available endpoints (/health, /health/live, /health/ready)
  - Azure Web App configuration
  - Kubernetes probes setup
  - Monitoring and troubleshooting

### Deployment & Configuration
- **[AZURE_WEBAPP_CONFIG.md](./AZURE_WEBAPP_CONFIG.md)** - Azure Web App environment variable configuration
  - Required environment variables
  - Configuration methods (Portal, CLI, Bicep)
  - Security best practices
  - Troubleshooting guide

- **[DEPLOYMENT_CHECKLIST.md](./DEPLOYMENT_CHECKLIST.md)** - Step-by-step deployment checklist
  - Pre-deployment preparation
  - Configuration verification
  - Post-deployment testing
  - Common issues and solutions

### Token Management
- **[TOKEN_CACHING_GUIDE.md](./TOKEN_CACHING_GUIDE.md)** - Databricks token caching implementation
  - How token caching works
  - Performance impact (75% faster)
  - Local vs production behavior
  - Troubleshooting

### Testing
- **[TESTING_QUICK_REFERENCE.md](./TESTING_QUICK_REFERENCE.md)** - Quick testing reference
  - Test commands
  - Test structure
  - Common patterns

- **[TEST_SUITE_SUMMARY.md](./TEST_SUITE_SUMMARY.md)** - Comprehensive test suite overview
  - Test organization
  - Coverage details
  - Test fixtures

- **[FIXTURE_ORGANIZATION.md](./FIXTURE_ORGANIZATION.md)** - Test fixture documentation
  - Fixture structure
  - Mock setup
  - Usage examples

### CI/CD
- **[CI_FIX_GUIDE.md](./CI_FIX_GUIDE.md)** - GitHub Actions CI troubleshooting
  - Common CI failures
  - Dependency fixes
  - Workflow examples

### Logging
- **[README_LOGGING.md](./README_LOGGING.md)** - Logging system documentation
  - Azure Blob Storage logging
  - PostgreSQL logging
  - Log configuration

- **[DATABASE_LOGGING_GUIDE.md](./DATABASE_LOGGING_GUIDE.md)** - Database logging setup with request tracking
  - PostgreSQL setup in Azure
  - Tracking who/where/when for every request
  - User identity detection (Azure AD, JWT, API keys)
  - Example queries for auditing and analytics
  - Security best practices

- **[QUICK_START_DATABASE_LOGGING.md](./QUICK_START_DATABASE_LOGGING.md)** - 5-minute PostgreSQL setup
  - Quick setup for database logging
  - Essential queries
  - User identity tracking

- **[AZURE_BLOB_LOGGING_GUIDE.md](./AZURE_BLOB_LOGGING_GUIDE.md)** - Blob storage logging for analytics
  - Structured JSON logging to Azure Blob Storage
  - Automatic date/time partitioning
  - External table setup (Synapse, Databricks, Kusto)
  - Request/response tracking with full context
  - Cost optimization with lifecycle policies

- **[QUICK_START_BLOB_LOGGING.md](./QUICK_START_BLOB_LOGGING.md)** - 5-minute blob storage setup
  - Quick setup for blob logging
  - External table examples
  - Common analytics queries

## 🚀 Quick Start

1. **Local Development**
   - Read [CLAUDE.md](./CLAUDE.md) for project overview
   - Run `make install` to set up
   - Run `make test` to verify setup

2. **Azure Deployment**
   - Follow [DEPLOYMENT_CHECKLIST.md](./DEPLOYMENT_CHECKLIST.md)
   - Configure variables per [AZURE_WEBAPP_CONFIG.md](./AZURE_WEBAPP_CONFIG.md)
   - Verify token caching per [TOKEN_CACHING_GUIDE.md](./TOKEN_CACHING_GUIDE.md)

3. **CI/CD Setup**
   - Fix issues using [CI_FIX_GUIDE.md](./CI_FIX_GUIDE.md)
   - Run tests: `bash run.sh test:ci`

## 📋 Key Features Documented

✅ **Environment Configuration** - Complete guide for local and Azure Web App
✅ **Token Caching** - Automatic token management and caching
✅ **Testing Framework** - Comprehensive test suite with 74 tests
✅ **Logging System** - Multi-destination logging (Azure Blob, PostgreSQL)
✅ **CI/CD Integration** - GitHub Actions workflow setup

## 🔧 Maintenance

These documentation files are maintained alongside the codebase and should be updated when:
- New features are added
- Configuration requirements change
- Deployment process is modified
- Testing patterns evolve

## 📝 Recent Updates

- **2026-01-04**: Added token caching implementation and CI fixes
- **2026-01-03**: Initial deployment and configuration guides
- **2026-01-02**: Test suite and logging documentation

---

For questions or issues, refer to the specific documentation file above or check the main [CLAUDE.md](./CLAUDE.md) for project context.
