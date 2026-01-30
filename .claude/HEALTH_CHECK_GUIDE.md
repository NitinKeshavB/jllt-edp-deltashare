# Health Check API Guide

The Delta Share API now includes comprehensive health check endpoints for monitoring application status and availability.

## 📋 Available Endpoints

### 1. **Basic Health Check** - `/health`

Simple health check that returns application status and metadata.

**Response (200 OK):**
```json
{
  "status": "healthy",
  "timestamp": "2026-01-05T12:00:00.000000Z",
  "service": "Delta Share API",
  "version": "v1",
  "workspace_url": "https://adb-xxx.azuredatabricks.net/"
}
```

**Use Cases:**
- Azure Web App health monitoring
- General application status checks
- Load balancer health probes

**Characteristics:**
- ✅ Lightweight - no dependency checks
- ✅ Always returns 200 OK if app is running
- ✅ Includes basic metadata

---

### 2. **Liveness Check** - `/health/live`

Minimal endpoint to verify the application process is running.

**Response (200 OK):**
```json
{
  "status": "alive",
  "timestamp": "2026-01-05T12:00:00.000000Z"
}
```

**Use Cases:**
- Kubernetes liveness probes
- Container orchestration platforms
- Determining if app should be restarted

**Characteristics:**
- ✅ Ultra-lightweight - minimal processing
- ✅ No dependency checks
- ✅ Always succeeds if app is running

---

### 3. **Readiness Check** - `/health/ready`

Verifies the application is ready to serve requests by checking critical dependencies.

**Response (200 OK) - Ready:**
```json
{
  "status": "ready",
  "timestamp": "2026-01-05T12:00:00.000000Z",
  "service": "Delta Share API",
  "checks": {
    "settings": "ok",
    "authentication": "ok"
  }
}
```

**Response (503 Service Unavailable) - Not Ready:**
```json
{
  "status": "not_ready",
  "timestamp": "2026-01-05T12:00:00.000000Z",
  "service": "Delta Share API",
  "checks": {
    "settings": "ok",
    "authentication": "failed"
  },
  "error": "Authentication credentials not configured"
}
```

**Use Cases:**
- Kubernetes readiness probes
- Load balancer routing decisions
- Deployment verification

**Checks Performed:**
1. ✅ Settings are loaded (`dltshr_workspace_url`)
2. ✅ Authentication credentials configured (`client_id`, `client_secret`, `account_id`)

**Characteristics:**
- ✅ Returns 200 OK when ready
- ✅ Returns 503 when not ready
- ✅ Provides detailed check results

---

## 🚀 Azure Web App Configuration

### Configure Health Check in Azure Portal

1. Navigate to your Web App in Azure Portal
2. Go to **Settings** → **Health check**
3. Enable health check
4. Set the path to: `/health`
5. Save configuration

### Configure via Azure CLI

```bash
az webapp config set \
  --name agenticops \
  --resource-group <your-resource-group> \
  --health-check-path "/health"
```

### Configure in Bicep/ARM Template

```bicep
resource webApp 'Microsoft.Web/sites@2022-03-01' = {
  name: 'agenticops'
  properties: {
    siteConfig: {
      healthCheckPath: '/health'
    }
  }
}
```

---

## 🐳 Kubernetes Configuration

### Liveness Probe

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: deltashare-api
spec:
  containers:
  - name: api
    image: deltashare-api:latest
    livenessProbe:
      httpGet:
        path: /health/live
        port: 8000
      initialDelaySeconds: 30
      periodSeconds: 10
      timeoutSeconds: 5
      failureThreshold: 3
```

### Readiness Probe

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: deltashare-api
spec:
  containers:
  - name: api
    image: deltashare-api:latest
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8000
      initialDelaySeconds: 10
      periodSeconds: 5
      timeoutSeconds: 3
      successThreshold: 1
      failureThreshold: 3
```

---

## 🧪 Testing Health Endpoints

### Using cURL

```bash
# Basic health check
curl http://localhost:8000/health

# Liveness check
curl http://localhost:8000/health/live

# Readiness check
curl http://localhost:8000/health/ready
```

### Using Python requests

```python
import requests

# Health check
response = requests.get("http://localhost:8000/health")
print(f"Status: {response.json()['status']}")

# Readiness check
response = requests.get("http://localhost:8000/health/ready")
if response.status_code == 200:
    print("Application is ready!")
else:
    print(f"Not ready: {response.json()['error']}")
```

### Automated Tests

All health check endpoints have comprehensive test coverage:

```bash
# Run health check tests
pytest tests/test_routes_health.py -v

# Run with coverage
pytest tests/test_routes_health.py --cov=src/dbrx_api/routes_health
```

---

## 📊 Monitoring Integration

### Azure Application Insights

Health check endpoints automatically log to Azure Application Insights (if configured):

```python
# Logs generated:
# - Health check requests
# - Readiness check results
# - Failed readiness checks with error details
```

### Prometheus/Grafana

You can scrape health endpoints for metrics:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'deltashare-api'
    metrics_path: '/health'
    static_configs:
      - targets: ['deltashare-api:8000']
```

---

## 🔍 Troubleshooting

### Health Check Returns 503

**Check the response for error details:**
```bash
curl -i http://localhost:8000/health/ready
```

**Common Issues:**

1. **Missing Workspace URL**
   - Error: `"Workspace URL not configured"`
   - Fix: Set `DLTSHR_WORKSPACE_URL` environment variable

2. **Missing Authentication Credentials**
   - Error: `"Authentication credentials not configured"`
   - Fix: Set `CLIENT_ID`, `CLIENT_SECRET`, `ACCOUNT_ID` environment variables

### Health Check Not Working in Azure

1. Verify the health check path is `/health` (not `/health/`)
2. Check Application Logs for errors
3. Ensure the app is listening on port 8000
4. Verify environment variables are configured

---

## 📝 Best Practices

1. **Use `/health` for Azure Web Apps** - Simple, reliable health check
2. **Use `/health/live` for container liveness** - Minimal overhead
3. **Use `/health/ready` for load balancer routing** - Ensures app can serve traffic
4. **Monitor health endpoint response times** - Should be < 100ms
5. **Set appropriate timeouts** - 3-5 seconds for readiness, 1-2 seconds for liveness
6. **Don't perform expensive checks** - Keep health checks lightweight

---

## 🎯 Summary

| Endpoint | Use Case | Response Time | Checks Dependencies |
|----------|----------|---------------|---------------------|
| `/health` | General monitoring | ~10ms | ❌ No |
| `/health/live` | Container liveness | ~5ms | ❌ No |
| `/health/ready` | Load balancer routing | ~20ms | ✅ Yes |

All endpoints return JSON and include ISO 8601 timestamps in UTC timezone.
