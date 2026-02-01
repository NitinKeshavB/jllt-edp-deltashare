# Postman Collections for Delta Share API

This folder contains Postman collections for testing the Delta Share API endpoints.

## Available Collections

### 1. Recipients API Collection
**File:** `Recipients_API.postman_collection.json`

Complete collection for managing Delta Share recipients (both D2D and D2O types).

## How to Import into Postman

1. Open Postman
2. Click **Import** button (top left)
3. Select the JSON file from this folder
4. Click **Import**

## Configuration

After importing, you need to configure the collection variables:

### Collection Variables

Go to the collection → **Variables** tab and update:

| Variable | Default Value | Description |
|----------|---------------|-------------|
| `base_url` | `http://localhost:8000` | Your API base URL |
| `workspace_url` | `https://adb-xxxx.azuredatabricks.net` | Your Databricks workspace URL |
| `subscription_key` | `your-subscription-key-here` | Azure APIM subscription key |
| `recipient_name` | `test_recipient` | Default recipient name for testing |

### Setting Variables

**Option 1: Collection Level (Recommended)**
1. Click on the collection name
2. Go to **Variables** tab
3. Update the **Current Value** column
4. Click **Save**

**Option 2: Environment (for multiple environments)**
1. Create a new Environment (e.g., "Dev", "Staging", "Production")
2. Add the same variables
3. Select the environment from the dropdown (top right)

## Recipients API Endpoints

### Included Endpoints (11 total)

#### Query Operations
1. **List All Recipients** - `GET /recipients`
   - Optional prefix filtering
   - Pagination support

2. **Get Recipient by Name** - `GET /recipients/{recipient_name}`
   - Returns detailed recipient information

#### Create Operations
3. **Create D2D Recipient** - `POST /recipients/d2d/{recipient_name}`
   - Databricks-to-Databricks sharing
   - Requires: `recipient_identifier` (format: `cloud:region:uuid`)
   - Example: `azure:eastus:12345678-1234-1234-1234-123456789012`

4. **Create D2O Recipient** - `POST /recipients/d2o/{recipient_name}`
   - Databricks-to-Open sharing
   - Returns activation URL and access tokens
   - Optional: IP access list (IPs or CIDR blocks)

#### Delete Operations
5. **Delete Recipient** - `DELETE /recipients/{recipient_name}`
   - User must be the owner

#### Update Operations (D2O Recipients)
6. **Rotate Token** - `PUT /recipients/{recipient_name}/tokens/rotate`
   - Only for TOKEN authentication type
   - Optional expiration time

7. **Add IP Addresses** - `PUT /recipients/{recipient_name}/ipaddress/add`
   - Add IPs or CIDR blocks to allow list
   - Only for TOKEN authentication

8. **Revoke IP Addresses** - `PUT /recipients/{recipient_name}/ipaddress/revoke`
   - Remove IPs from allow list
   - Only for TOKEN authentication

#### Update Operations (All Recipients)
9. **Update Description** - `PUT /recipients/{recipient_name}/description/update`
   - User must be the owner

10. **Update Expiration Time** - `PUT /recipients/{recipient_name}/expiration_time/update`
    - Only for TOKEN authentication (D2O)
    - Time specified in days

## Example Usage

### Creating a D2D Recipient

```
POST {{base_url}}/recipients/d2d/partner_company
Headers:
  X-Workspace-URL: {{workspace_url}}
  Ocp-Apim-Subscription-Key: {{subscription_key}}
Query Parameters:
  recipient_identifier: azure:eastus:12345678-1234-1234-1234-123456789012
  description: D2D recipient for partner company
  sharing_code: (optional)
```

### Creating a D2O Recipient with IP Access List

```
POST {{base_url}}/recipients/d2o/external_partner
Headers:
  X-Workspace-URL: {{workspace_url}}
  Ocp-Apim-Subscription-Key: {{subscription_key}}
Query Parameters:
  description: External partner with restricted IPs
  ip_access_list: 192.168.1.100
  ip_access_list: 10.0.0.0/24
```

### Rotating a Token

```
PUT {{base_url}}/recipients/external_partner/tokens/rotate
Headers:
  X-Workspace-URL: {{workspace_url}}
  Ocp-Apim-Subscription-Key: {{subscription_key}}
Query Parameters:
  expire_in_seconds: 0
```

## Common Response Codes

| Code | Status | Description |
|------|--------|-------------|
| 200 | OK | Request successful |
| 201 | Created | Resource created successfully |
| 400 | Bad Request | Invalid request parameters |
| 403 | Forbidden | Permission denied (not owner) |
| 404 | Not Found | Recipient not found |
| 409 | Conflict | Recipient already exists |

## Authentication

All requests require two headers:

1. **X-Workspace-URL**: Your Databricks workspace URL
   - Format: `https://adb-xxxxx.azuredatabricks.net`

2. **Ocp-Apim-Subscription-Key**: Your Azure APIM subscription key
   - Get this from Azure API Management portal

## IP Address Formats

When adding/revoking IP addresses, you can use:

1. **Single IP**: `192.168.1.100`
2. **CIDR Block**: `10.0.0.0/24`
3. **Multiple IPs**: Repeat the `ip_access_list` parameter

**Valid Examples:**
- `203.0.113.10`
- `198.51.100.0/24`
- `2001:db8::/32` (IPv6)

## D2D vs D2O Differences

### D2D (Databricks-to-Databricks)
- Uses `DATABRICKS` authentication type
- Requires `recipient_identifier` (metastore ID)
- **Does NOT support:**
  - IP access lists
  - Token rotation
  - Expiration time updates

### D2O (Databricks-to-Open)
- Uses `TOKEN` authentication type
- Returns activation URL and tokens
- **Supports:**
  - IP access lists (add/revoke)
  - Token rotation
  - Expiration time updates

## Tips

1. **Use Variables**: Leverage collection/environment variables for reusable values
2. **Save Responses**: Save example responses for documentation
3. **Test Scripts**: Add test scripts to validate responses
4. **Environments**: Create separate environments for Dev/Staging/Prod
5. **Folders**: Organize requests into folders for better structure

## Troubleshooting

### 401 Unauthorized
- Check your subscription key is correct
- Verify the key is active in Azure APIM

### 404 Workspace Not Found
- Verify the `X-Workspace-URL` header is set correctly
- Ensure the workspace URL is reachable

### 400 Invalid IP Address
- Check IP format (single IP or CIDR)
- Remove any extra spaces or quotes

### 403 Permission Denied
- You must be the owner of the recipient
- Check your service principal has the correct permissions

## Next Steps

1. Import the collection
2. Update collection variables
3. Test with "List All Recipients" first
4. Create test recipients
5. Experiment with update operations
6. Review the response schemas

## Additional Collections

More collections will be added for:
- Shares API
- Workflow API
- Health & Metrics

---

For API documentation, see the OpenAPI spec at `http://localhost:8000/` when the server is running.
