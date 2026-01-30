# Azure Web App Deployment Checklist

Use this checklist to ensure your Delta Share API is properly configured for Azure Web App deployment.

## Pre-Deployment

### 1. Databricks Configuration
- [ ] Databricks workspace URL is accessible
- [ ] Service Principal is created in Azure AD
- [ ] Service Principal has necessary permissions in Databricks workspace
- [ ] Client ID, Client Secret, and Account ID are documented

### 2. Azure Resources
- [ ] Azure Web App is created (Python 3.12)
- [ ] Azure Storage Account is created (if using blob logging)
- [ ] Azure PostgreSQL is created (if using database logging)
- [ ] Managed Identity is enabled on Web App (if using Azure resources)

### 3. Code Preparation
- [ ] All tests are passing (`bash run.sh run-tests`)
- [ ] Code is committed to repository
- [ ] `.env` file is in `.gitignore`
- [ ] No sensitive data in source code

## Deployment Configuration

### Required Environment Variables in Azure Web App
Set these in Azure Portal → Web App → Configuration → Application Settings:

- [ ] `DLTSHR_WORKSPACE_URL` = Your Databricks workspace URL
- [ ] `CLIENT_ID` = Service Principal Client ID
- [ ] `CLIENT_SECRET` = Service Principal Client Secret
- [ ] `ACCOUNT_ID` = Databricks Account ID

### Optional: Azure Blob Storage Logging
- [ ] `ENABLE_BLOB_LOGGING` = `true`
- [ ] `AZURE_STORAGE_ACCOUNT_URL` = `https://<account>.blob.core.windows.net`
- [ ] `AZURE_STORAGE_LOGS_CONTAINER` = `deltashare-logs` (or custom)
- [ ] Web App Managed Identity has "Storage Blob Data Contributor" role

### Optional: PostgreSQL Logging
- [ ] `ENABLE_POSTGRESQL_LOGGING` = `true`
- [ ] `POSTGRESQL_CONNECTION_STRING` = Connection string with credentials
- [ ] `POSTGRESQL_LOG_TABLE` = `application_logs` (or custom)
- [ ] `POSTGRESQL_MIN_LOG_LEVEL` = `WARNING` (or `ERROR`, `CRITICAL`)
- [ ] PostgreSQL firewall allows Azure services

## Deployment Methods

### Option 1: GitHub Actions (Recommended)
- [ ] GitHub repository is connected to Azure Web App
- [ ] Deployment workflow is configured
- [ ] Secrets are set in GitHub repository settings
- [ ] Workflow has completed successfully

### Option 2: Azure CLI
```bash
# Build and deploy
az webapp up \
  --runtime PYTHON:3.12 \
  --name <webapp-name> \
  --resource-group <resource-group>
```
- [ ] Deployment command executed successfully
- [ ] Build completed without errors

### Option 3: VS Code Azure Extension
- [ ] Azure extension is installed
- [ ] Signed in to Azure account
- [ ] Web App is selected
- [ ] Code is deployed

## Post-Deployment Verification

### 1. Application Health
- [ ] Web app is running (check Azure Portal)
- [ ] API docs are accessible: `https://<webapp>.azurewebsites.net/`
- [ ] No startup errors in Application Logs

### 2. Configuration Verification
Access the logs and verify:
```bash
az webapp log tail --resource-group <rg> --name <webapp-name>
```

Look for:
- [ ] `⚠ No .env file found - using web app environment variables` (expected)
- [ ] `✓ Settings loaded successfully`
- [ ] `Starting DeltaShare API application`
- [ ] No authentication errors

### 3. Functionality Testing

Test basic endpoints:
```bash
# Health check (if implemented)
curl https://<webapp>.azurewebsites.net/

# List shares
curl https://<webapp>.azurewebsites.net/shares

# List recipients
curl https://<webapp>.azurewebsites.net/recipients
```

- [ ] API responds with 200 OK
- [ ] Authentication to Databricks works
- [ ] Responses contain expected data

### 4. Logging Verification (if enabled)

#### Azure Blob Storage
- [ ] Container `deltashare-logs` exists
- [ ] Log files are being created with structure: `YYYY/MM/DD/HH/log_*.json`
- [ ] Logs contain proper JSON data

#### PostgreSQL
- [ ] Table `application_logs` exists
- [ ] Logs are being inserted
- [ ] Only WARNING and above are logged (if min_log_level=WARNING)

## Security Verification

- [ ] `.env` file is NOT in repository
- [ ] Sensitive values are NOT in code
- [ ] HTTPS is enforced
- [ ] Authentication headers are not logged
- [ ] Web App has minimum required permissions

## Performance & Monitoring

- [ ] Application Insights is configured (recommended)
- [ ] Alerts are set up for errors
- [ ] Auto-scaling is configured (if needed)
- [ ] Health check endpoint is configured

## Rollback Plan

In case of issues:
- [ ] Previous working deployment is tagged/documented
- [ ] Rollback procedure is documented:
  ```bash
  # Rollback to previous deployment
  az webapp deployment source config-zip \
    --resource-group <rg> \
    --name <webapp-name> \
    --src <previous-version.zip>
  ```

## Common Issues & Solutions

### Issue: "Field required" error
**Solution:** Missing environment variable in Web App configuration
- Check all required variables are set
- Restart web app after adding variables

### Issue: Authentication fails to Databricks
**Solution:** Invalid credentials
- Verify CLIENT_ID, CLIENT_SECRET, ACCOUNT_ID
- Check Service Principal has correct permissions
- Check DLTSHR_WORKSPACE_URL is correct

### Issue: Application won't start
**Solution:** Check logs
```bash
az webapp log tail --resource-group <rg> --name <webapp-name>
```
- Look for Python errors
- Check all dependencies are in requirements.txt
- Verify Python version (3.12+)

### Issue: Logs not appearing in Azure Storage
**Solution:** Check permissions
- Ensure Managed Identity is enabled
- Verify "Storage Blob Data Contributor" role
- Check AZURE_STORAGE_ACCOUNT_URL is correct

## Final Checklist

- [ ] All environment variables configured
- [ ] Application deployed successfully
- [ ] API is accessible and responding
- [ ] Authentication to Databricks works
- [ ] Logging is working (if enabled)
- [ ] No errors in application logs
- [ ] Documentation is updated
- [ ] Team is notified of deployment

## Documentation Complete?

- [ ] API documentation is accessible
- [ ] Environment variables documented
- [ ] Deployment process documented
- [ ] Support contacts documented
- [ ] Known issues documented

---

**Deployment Date:** _________________

**Deployed By:** _________________

**Web App URL:** _________________

**Notes:**
