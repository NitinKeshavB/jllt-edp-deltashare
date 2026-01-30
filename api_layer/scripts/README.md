# Azure APIM OpenAPI Generation

This directory contains the generated OpenAPI specifications for Azure API Management (APIM) compatibility.

## Overview

The `scripts/generate_openapi.py` script creates OpenAPI 3.0.3 specifications from the FastAPI application with Azure APIM-specific enhancements:

- **Security**: Adds proper Azure APIM subscription key authentication
- **Workspace URLs**: Includes required `X-Workspace-URL` header for Databricks operations
- **Environment-specific servers**: Different server configurations for dev/uat/prod
- **Clean endpoints**: Removes internal endpoints not suitable for public API
- **Enhanced metadata**: Rich descriptions and proper tagging for better APIM organization

## Usage

### Basic Usage

```bash
# Generate OpenAPI spec for development environment
python scripts/generate_openapi.py

# Generate for production environment
python scripts/generate_openapi.py --env prod

# Generate with pretty formatting
python scripts/generate_openapi.py --pretty

# Custom output path
python scripts/generate_openapi.py --output custom/path/api.json
```

### Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--output` | `-o` | Output file path | `apim_openapi/openapi.json` |
| `--env` | `-e` | Environment (dev/uat/prod) | `dev` |
| `--pretty` | `-p` | Pretty print JSON | `false` |
| `--skip-auth-validation` | | Skip auth validation (testing only) | `false` |

### Environment-Specific Configuration

The script generates different server configurations based on the environment:

#### Development (`--env dev`)
- Azure APIM development instance
- Local development server (localhost:8000)

#### UAT (`--env uat`)
- Azure APIM UAT instance

#### Production (`--env prod`)
- Azure APIM production instance

## Requirements

The script requires the FastAPI application to be importable and proper credentials:

### 1. Dependencies

```bash
make install
# or
pip install -e .[dev]
```

### 2. Credentials Configuration

#### **For Local Development**
Create a `.env` file in the `api_layer` directory:
```env
CLIENT_ID=your-azure-service-principal-client-id
CLIENT_SECRET=your-azure-service-principal-secret
ACCOUNT_ID=your-databricks-account-id
```

#### **For Azure Web App**
Set these as App Settings in Azure Web App Configuration:
```
CLIENT_ID = your-client-id
CLIENT_SECRET = your-secret
ACCOUNT_ID = your-account-id
```

#### **For CI/CD or Testing**
Use environment variables or the `--skip-auth-validation` flag:
```bash
# With environment variables
export CLIENT_ID=your-client-id
export CLIENT_SECRET=your-secret  
export ACCOUNT_ID=your-account-id
python scripts/generate_openapi.py

# For testing only (not recommended for production)
python scripts/generate_openapi.py --skip-auth-validation
```

### 3. Execution

```bash
cd api_layer
python scripts/generate_openapi.py
```

## Security

🔒 **Important Security Notes:**

- **Never hardcode credentials** in the script
- **Use environment variables** or `.env` files for local development
- **Use Azure App Settings** for deployed applications
- **The `--skip-auth-validation` flag** is for testing only and should not be used in production
- **Ensure `.env` files** are in `.gitignore` to prevent credential leaks

## Output

The generated OpenAPI specification includes:

### Security Schemes

1. **Azure APIM Subscription Key**: `Ocp-Apim-Subscription-Key` header
2. **Databricks Workspace URL**: `X-Workspace-URL` header

### API Sections

- **Health**: Health check and monitoring endpoints
- **Shares**: Delta Share management operations
- **Recipients**: Recipient management (D2D and D2O)
- **Pipelines**: DLT pipeline management and operations
- **Schedules**: Job scheduling and automation
- **Metrics**: Pipeline metrics and monitoring

### Enhanced Features

- **Azure APIM compatibility**: OpenAPI 3.0.3 format with proper security schemes
- **Operation IDs**: Clean, consistent operation identifiers
- **Tags**: Organized endpoint grouping
- **Rich descriptions**: Comprehensive API documentation
- **Server configurations**: Environment-specific server URLs

## Azure APIM Integration

To import the generated OpenAPI spec into Azure APIM:

1. **Generate the spec**:
   ```bash
   python scripts/generate_openapi.py --env prod --pretty
   ```

2. **Import to Azure APIM**:
   - Navigate to your Azure APIM instance
   - Go to APIs > Add a new API > OpenAPI
   - Upload the generated `openapi.json` file
   - Configure policies and rate limiting as needed

3. **Key Features**:
   - Subscription key validation is pre-configured
   - Health endpoints can bypass subscription key (for monitoring)
   - All endpoints require `X-Workspace-URL` header
   - Proper error responses and status codes

## File Structure

```
scripts/
├── generate_openapi.py     # Main generation script
└── README.md              # This file

apim_openapi/
└── openapi*.json          # Generated OpenAPI specs (after running script)
```

## Troubleshooting

### Import Errors

```bash
❌ Import error: No module named 'dbrx_api'
```

**Solution**: Ensure you're running from the `api_layer` directory and dependencies are installed:
```bash
cd api_layer
make install
python scripts/generate_openapi.py
```

### Credential Errors

```bash
❌ Failed to load credentials: [error details]
```

**Solution**: Ensure credentials are properly configured:

1. **Local development**: Check your `.env` file in the `api_layer` directory
2. **Azure Web App**: Verify App Settings are configured  
3. **Environment variables**: Ensure CLIENT_ID, CLIENT_SECRET, and ACCOUNT_ID are set
4. **Testing only**: Use `--skip-auth-validation` flag

### Missing Version

If the version can't be read from `version.txt`, the script defaults to `"1.0.0"`.

### Environment-Specific Issues

Make sure the environment parameter is one of: `dev`, `uat`, `prod`.

## Validation

The script performs basic validation:

- ✅ Required OpenAPI fields present
- ✅ Security schemes configured
- ✅ All endpoints have proper tags
- ✅ Operation IDs are consistent
- ✅ Server configurations are valid

For detailed OpenAPI validation, you can use external tools like [swagger-codegen](https://swagger.io/tools/swagger-codegen/) or [openapi-generator](https://openapi-generator.tech/).

## Make Commands

The following make commands are available for easier usage:

```bash
make generate-openapi          # Development environment
make generate-openapi-dev      # Development environment  
make generate-openapi-uat      # UAT environment
make generate-openapi-prod     # Production environment
make generate-openapi-all      # All environments
```
