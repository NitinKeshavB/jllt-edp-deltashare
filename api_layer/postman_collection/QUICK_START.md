# Quick Start Guide - Delta Share API Postman Collections

## Files Created/Updated

✅ **Recipients_API.postman_collection.json** - 11 endpoints (D2D/D2O recipient management)  
✅ **Pipelines_API.postman_collection.json** - 12 endpoints (DLT pipeline CRUD + updates)  
✅ **Schedule_API.postman_collection.json** - 7 endpoints (Cron job scheduling)  
✅ **Metrics_API.postman_collection.json** - 4 endpoints (Pipeline & job run metrics)  
✅ **Workflow_API.postman_collection.json** - 4 endpoints (Share pack workflows)  
✅ **Catalog_API.postman_collection.json** - 3 endpoints (Unity Catalog management)  
✅ **Shares_API.postman_collection.json** - 8 endpoints (Share management)  
✅ **Health_API.postman_collection.json** - 2 endpoints (Health checks)  

**Total: 48 endpoints across 8 collections**

## What Changed

All collections have been updated with:
1. ✅ `Ocp-Apim-Subscription-Key` header added to all requests
2. ✅ `base_url` changed from `http://localhost:8000` to `https://your-api.azurewebsites.net`
3. ✅ `subscription_key` variable added to all collections
4. ✅ Proper collection variables for entity names
5. ✅ Detailed descriptions for all endpoints

## 5-Minute Setup

### Step 1: Import Collections
```bash
# In Postman:
File > Import > Select all 8 .json files > Import
```

### Step 2: Configure Variables
Set these variables in each collection (or create an environment):

```
base_url: https://your-api.azurewebsites.net
workspace_url: https://adb-1234567890.12.azuredatabricks.net
subscription_key: your-actual-subscription-key
```

### Step 3: Test Connection
Run: `Health API > Get Health Status`

Expected response:
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2024-01-23T10:30:00Z"
}
```

## Common Workflows

### Workflow 1: Set Up Data Sharing
```
1. Create catalog (if needed)
   POST /catalogs/my_catalog

2. Create recipient
   POST /recipients/d2o/external_partner

3. Create share
   POST /shares/analytics_share

4. Add data to share
   PUT /shares/analytics_share/dataobject/add

5. Grant access to recipient
   PUT /shares/analytics_share/recipients/add
```

### Workflow 2: Create Scheduled Pipeline
```
1. Create pipeline
   POST /pipelines/daily_sync

2. Add notifications
   PUT /pipelines/daily_sync/notifications/add

3. Create schedule
   POST /pipelines/daily_sync/schedules

4. Monitor with metrics
   GET /pipelines/daily_sync/metrics
```

### Workflow 3: Manage D2O Recipient
```
1. Create D2O recipient
   POST /recipients/d2o/partner_name

2. Add IP restrictions
   PUT /recipients/partner_name/ipaddress/add

3. Set expiration
   PUT /recipients/partner_name/expiration_time/update

4. Rotate token (when needed)
   PUT /recipients/partner_name/tokens/rotate
```

## Collection-Specific Notes

### Recipients API
- **D2D**: Requires metastore ID (format: `cloud:region:uuid`)
- **D2O**: Supports IP allow lists (IPs or CIDR blocks)
- Token rotation only works for D2O recipients

### Pipelines API
- Configuration validation happens before creation
- Keys are validated against source table columns
- Notifications support both emails and AD groups
- Full refresh deletes all existing data

### Schedule API
- Uses Quartz cron (6 fields): `sec min hour day month day-of-week`
- Example: `0 0 12 * * ?` = Daily at noon UTC
- Timezone aware (supports all standard timezones)

### Metrics API
- Timestamp filter is optional
- ISO 8601 format: `2024-01-23T10:30:00Z`
- Returns comprehensive run history

### Workflow API
- Supports YAML and Excel uploads
- Async processing (returns 202 Accepted)
- Check status with share_pack_id

## Troubleshooting

### Issue: 401 Unauthorized
**Solution**: Check `subscription_key` is set correctly

### Issue: 404 Workspace not found
**Solution**: Verify `workspace_url` format and DNS resolution

### Issue: Pipeline validation fails
**Solution**: Ensure source table exists and keys match column names

### Issue: Cron expression invalid
**Solution**: Use 6 fields (not 5 or 7). Example: `0 0 12 * * ?`

### Issue: D2D recipient creation fails
**Solution**: Verify metastore ID format: `cloud:region:uuid`

## Next Steps

1. ✅ Import all collections
2. ✅ Configure variables
3. ✅ Test health endpoint
4. ✅ Try common workflows
5. ✅ Check API docs at `https://your-api.azurewebsites.net/`

## Additional Resources

- **Full Documentation**: See `COLLECTION_SUMMARY.md`
- **API Docs**: `https://your-api.azurewebsites.net/` (Swagger UI)
- **ReDoc**: `https://your-api.azurewebsites.net/redoc`
- **Project README**: `../README.md`

---

**Created**: 2024-02-02  
**Collections Version**: v2.1.0  
**Total Endpoints**: 48  
**Status**: ✅ Ready for use
