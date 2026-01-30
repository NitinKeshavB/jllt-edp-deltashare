# Azure Web App Configuration Guide

This guide explains how to configure environment variables for the Delta Share API when deploying to Azure Web App.

## Environment Variables Configuration

The application automatically reads configuration from:
1. **Azure Web App Environment Variables** (Production)
2. **`.env` file** (Local Development)

### Required Environment Variables

Configure these in your Azure Web App → Configuration → Application Settings:

#### Databricks Configuration
| Variable Name | Description | Example |
|--------------|-------------|---------|
| `DLTSHR_WORKSPACE_URL` | Databricks workspace URL | `https://adb-xxxxx.azuredatabricks.net/` |
| `CLIENT_ID` | Azure Service Principal Client ID | `e04058ec-8264-440d-a49d-25b31ac1b9ca` |
| `CLIENT_SECRET` | Azure Service Principal Client Secret | `dose5397a24ce2fe1950f3b9ff405b20b6f0` |
| `ACCOUNT_ID` | Databricks Account ID | `5d7e2283-aa22-47dc-9cb2-bc52216be1e9` |

#### Optional: Logging Configuration (Azure Blob Storage)
| Variable Name | Description | Example | Default |
|--------------|-------------|---------|---------|
| `ENABLE_BLOB_LOGGING` | Enable Azure Blob Storage logging | `true` or `false` | `false` |
| `AZURE_STORAGE_ACCOUNT_URL` | Azure Storage Account URL | `https://mystorageaccount.blob.core.windows.net` | `None` |
| `AZURE_STORAGE_LOGS_CONTAINER` | Blob container name for logs | `deltashare-logs` | `deltashare-logs` |

#### Optional: Logging Configuration (PostgreSQL)
| Variable Name | Description | Example | Default |
|--------------|-------------|---------|---------|
| `ENABLE_POSTGRESQL_LOGGING` | Enable PostgreSQL logging | `true` or `false` | `false` |
| `POSTGRESQL_CONNECTION_STRING` | PostgreSQL connection string | `postgresql://user:pass@host:5432/db` | `None` |
| `POSTGRESQL_LOG_TABLE` | PostgreSQL table name for logs | `application_logs` | `application_logs` |
| `POSTGRESQL_MIN_LOG_LEVEL` | Minimum log level for PostgreSQL | `WARNING`, `ERROR`, `CRITICAL` | `WARNING` |

#### Optional: Token Caching (Auto-managed)
| Variable Name | Description | Notes |
|--------------|-------------|-------|
| `DATABRICKS_TOKEN` | Cached OAuth token | Auto-generated, no need to set |
| `TOKEN_EXPIRES_AT_UTC` | Token expiration time | Auto-managed, no need to set |

## How to Configure in Azure Web App

### Method 1: Azure Portal
1. Navigate to your Azure Web App
2. Go to **Configuration** → **Application settings**
3. Click **+ New application setting**
4. Add each environment variable with its value
5. Click **Save** and **Restart** the web app

### Method 2: Azure CLI
```bash
az webapp config appsettings set \
  --resource-group <resource-group-name> \
  --name <webapp-name> \
  --settings \
    DLTSHR_WORKSPACE_URL="https://adb-xxxxx.azuredatabricks.net/" \
    CLIENT_ID="your-client-id" \
    CLIENT_SECRET="your-client-secret" \
    ACCOUNT_ID="your-account-id" \
    ENABLE_BLOB_LOGGING="true" \
    AZURE_STORAGE_ACCOUNT_URL="https://mystorageaccount.blob.core.windows.net" \
    ENABLE_POSTGRESQL_LOGGING="true" \
    POSTGRESQL_CONNECTION_STRING="postgresql://user:pass@host:5432/db"
```

### Method 3: Bicep/ARM Template
```bicep
resource webApp 'Microsoft.Web/sites@2022-03-01' = {
  name: webAppName
  location: location
  properties: {
    siteConfig: {
      appSettings: [
        {
          name: 'DLTSHR_WORKSPACE_URL'
          value: 'https://adb-xxxxx.azuredatabricks.net/'
        }
        {
          name: 'CLIENT_ID'
          value: clientId
        }
        {
          name: 'CLIENT_SECRET'
          value: clientSecret
        }
        {
          name: 'ACCOUNT_ID'
          value: accountId
        }
        {
          name: 'ENABLE_BLOB_LOGGING'
          value: 'true'
        }
        {
          name: 'AZURE_STORAGE_ACCOUNT_URL'
          value: storageAccountUrl
        }
        {
          name: 'ENABLE_POSTGRESQL_LOGGING'
          value: 'true'
        }
        {
          name: 'POSTGRESQL_CONNECTION_STRING'
          value: postgresqlConnectionString
        }
      ]
    }
  }
}
```

## Local Development

For local development, create a `.env` file in the project root with all required variables:

```bash
# .env file (DO NOT COMMIT TO VERSION CONTROL)
DLTSHR_WORKSPACE_URL=https://adb-xxxxx.azuredatabricks.net/
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
ACCOUNT_ID=your-account-id

# Optional logging
ENABLE_BLOB_LOGGING=true
AZURE_STORAGE_ACCOUNT_URL=https://mystorageaccount.blob.core.windows.net
ENABLE_POSTGRESQL_LOGGING=true
POSTGRESQL_CONNECTION_STRING=postgresql://user:pass@host:5432/db
```

**Note:** The `.env` file is already listed in `.gitignore` and should never be committed to version control.

## How It Works

The application uses **Pydantic Settings** which automatically:
1. Checks for environment variables in the system (Azure Web App)
2. Falls back to `.env` file if running locally
3. Validates all required variables are present
4. Provides type checking and conversion

### Settings Class
All configuration is centralized in `src/dbrx_api/settings.py`:

```python
from dbrx_api.settings import Settings

# Automatically loads from environment or .env file
settings = Settings()

# Access configuration
workspace_url = settings.dltshr_workspace_url
client_id = settings.client_id
```

### Variable Name Flexibility
All variable names are **case-insensitive**:
- `DLTSHR_WORKSPACE_URL` = `dltshr_workspace_url` = `DltShr_Workspace_Url`

## Troubleshooting

### Error: "Field required"
- **Cause:** A required environment variable is missing
- **Solution:** Add the missing variable to Azure Web App configuration

### Error: "No .env file found - using web app environment variables"
- **Status:** This is normal in production
- **Action:** No action needed - environment variables are being read from Azure Web App

### Token Generation Errors
- **Cause:** Invalid `CLIENT_ID`, `CLIENT_SECRET`, or `ACCOUNT_ID`
- **Solution:** Verify the service principal credentials in Azure AD

### Logging Not Working
- **Cause:** Missing Azure Storage or PostgreSQL configuration
- **Solution:**
  1. Set `ENABLE_BLOB_LOGGING=true` or `ENABLE_POSTGRESQL_LOGGING=true`
  2. Configure the corresponding connection strings
  3. Ensure the web app has appropriate permissions (Managed Identity for Azure Storage)

## Security Best Practices

1. **Never commit `.env` file** to version control
2. **Use Azure Key Vault** for sensitive values in production:
   ```bash
   @Microsoft.KeyVault(SecretUri=https://myvault.vault.azure.net/secrets/ClientSecret/)
   ```
3. **Enable Managed Identity** for Azure resources (Storage, PostgreSQL)
4. **Rotate credentials** regularly
5. **Use separate credentials** for dev/staging/production environments

## Verification

After deploying, verify configuration by checking the logs:
```bash
az webapp log tail --resource-group <rg> --name <webapp-name>
```

Look for:
- ✓ Settings loaded successfully
- ✓ Loaded environment variables from .env (local only)
- ⚠ No .env file found - using web app environment variables (production)

## Additional Resources

- [Azure Web App Configuration](https://learn.microsoft.com/en-us/azure/app-service/configure-common)
- [Pydantic Settings Documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
- [Databricks Service Principal Authentication](https://docs.databricks.com/dev-tools/auth.html#azure-service-principal-authentication)
