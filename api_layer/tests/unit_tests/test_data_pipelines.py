"""Test data and constants for pipeline testing."""

# Pipeline Names
VALID_PIPELINE_NAMES = [
    "dlt-pattern-load-citibike",
    "streaming-pipeline-prod",
    "batch_etl_daily",
    "test_pipeline_123",
]

INVALID_PIPELINE_NAMES = [
    "",  # Empty
    "pipeline-with-very-long-name" * 10,  # Too long
    "pipeline with spaces",  # Spaces not allowed
    "pipeline@special",  # Special characters
]

# Pipeline Configurations
VALID_CONFIGURATIONS = [
    {
        "pipelines.source_table": "catalog.schema.source",
        "pipelines.keys": "id",
        "pipelines.target_table": "target_table",
        "pipelines.scd_type": "1",
    },
    {
        "pipelines.source_table": "dltshr_prod.bronze.rides",
        "pipelines.keys": "ride_id,timestamp",
        "pipelines.target_table": "silver_rides_scd",
        "pipelines.scd_type": "2",
    },
    {
        "pipelines.source_table": "catalog.schema.events",
        "pipelines.keys": "event_id",
        "pipelines.target_table": "processed_events",
        "pipelines.scd_type": "1",
        "pipelines.custom_param": "custom_value",
    },
]

INVALID_CONFIGURATIONS = [
    {
        # Missing required keys
        "pipelines.source_table": "catalog.schema.source",
    },
    {
        # Invalid source table format
        "pipelines.source_table": "invalid_table",
        "pipelines.keys": "id",
        "pipelines.target_table": "target",
        "pipelines.scd_type": "1",
    },
    {
        # Invalid scd_type
        "pipelines.source_table": "catalog.schema.source",
        "pipelines.keys": "id",
        "pipelines.target_table": "target",
        "pipelines.scd_type": "3",  # Only 1 or 2 allowed
    },
]

# Notification Lists
VALID_NOTIFICATION_LISTS = [
    ["user@example.com"],
    ["admin@example.com", "team@example.com"],
    ["data-engineering-team", "admin@example.com"],
    ["user.name@company.com", "ad-group-name", "another.user@domain.co.uk"],
]

INVALID_NOTIFICATION_LISTS = [
    [],  # Empty list
    ["invalid-email"],  # Not an email or AD group
    ["user@", "@domain.com"],  # Malformed emails
    ["group with spaces"],  # AD group with spaces
]

# Tags
VALID_TAGS = [
    {"env": "prod"},
    {"env": "dev", "team": "data-engineering"},
    {
        "environment": "production",
        "team": "analytics",
        "project": "deltashare",
        "cost_center": "data-platform",
        "owner": "admin@example.com",
    },
    {
        "key_with_underscores": "value",
        "key-with-hyphens": "value",
        "key.with.dots": "value",
    },
]

INVALID_TAGS = [
    {},  # Empty dict
    {"key with spaces": "value"},  # Invalid key
    {"valid_key": ""},  # Empty value
    {"key": "v" * 300},  # Value too long (>256 chars)
    {"invalid@key": "value"},  # Invalid character in key
]

# Pipeline States
PIPELINE_STATES = {
    "idle": "IDLE",
    "running": "RUNNING",
    "stopping": "STOPPING",
    "stopped": "STOPPED",
    "failed": "FAILED",
    "starting": "STARTING",
    "resetting": "RESETTING",
    "deleted": "DELETED",
}

# Catalog and Schema Names
VALID_CATALOGS = [
    "dltshr_prod",
    "dltshr_dev",
    "catalog123",
    "my_catalog",
]

INVALID_CATALOGS = [
    "",  # Empty
    "catalog with spaces",
    "catalog@special",
]

VALID_SCHEMAS = [
    "01_bronze",
    "02_silver",
    "03_gold",
    "schema_123",
    "my_schema",
]

INVALID_SCHEMAS = [
    "",  # Empty
    "schema with spaces",
    "schema@special",
]

# Library Paths
VALID_LIBRARY_PATHS = [
    "/Workspace/pipelines/etl.py",
    "/Workspace/Shared/dlt/pipeline.py",
    "/Repos/user/project/pipeline.py",
    "/Workspace/users/admin@example.com/pipeline.py",
]

INVALID_LIBRARY_PATHS = [
    "/invalid/path.py",  # Not in Workspace or Repos
    "/Workspace/pipelines/notebook",  # Missing .py extension
    "relative/path.py",  # Not absolute
    "",  # Empty
]

# Full Refresh Scenarios
FULL_REFRESH_SCENARIOS = [
    {
        "name": "idle_pipeline",
        "initial_state": "IDLE",
        "expected_stops": 0,
        "expected_result": "success",
    },
    {
        "name": "stopped_pipeline",
        "initial_state": "STOPPED",
        "expected_stops": 0,
        "expected_result": "success",
    },
    {
        "name": "running_pipeline",
        "initial_state": "RUNNING",
        "expected_stops": 1,
        "expected_result": "success",
    },
    {
        "name": "starting_pipeline",
        "initial_state": "STARTING",
        "expected_stops": 1,
        "expected_result": "success",
    },
    {
        "name": "stopping_pipeline_timeout",
        "initial_state": "STOPPING",
        "expected_stops": 1,
        "expected_result": "timeout",
        "final_state": "STOPPING",
    },
]

# Continuous Mode Scenarios
CONTINUOUS_MODE_SCENARIOS = [
    {
        "name": "enable_continuous",
        "current_continuous": False,
        "new_continuous": True,
        "expected_mode": "continuous",
    },
    {
        "name": "disable_continuous",
        "current_continuous": True,
        "new_continuous": False,
        "expected_mode": "triggered",
    },
    {
        "name": "no_change_true",
        "current_continuous": True,
        "new_continuous": True,
        "expected_mode": "continuous",
    },
    {
        "name": "no_change_false",
        "current_continuous": False,
        "new_continuous": False,
        "expected_mode": "triggered",
    },
]

# Error Messages
ERROR_MESSAGES = {
    "pipeline_not_found": "Pipeline not found",
    "permission_denied": "Permission denied",
    "not_an_owner": "User is not an owner",
    "timeout": "did not stop within 600 seconds",
    "invalid_configuration": "Invalid configuration",
    "missing_required_field": "Field required",
    "validation_error": "validation error",
}

# Sample Complete Pipeline Definitions
SAMPLE_PIPELINES = [
    {
        "pipeline_name": "citibike-streaming-prod",
        "target_catalog_name": "dltshr_prod",
        "target_schema_name": "02_silver",
        "configuration": {
            "pipelines.source_table": "dltshr_prod.01_bronze.citibike_raw",
            "pipelines.keys": "ride_id",
            "pipelines.target_table": "citibike_rides_scd",
            "pipelines.scd_type": "2",
        },
        "notifications_list": ["data-engineering-team@example.com"],
        "tags": {
            "env": "prod",
            "team": "data-engineering",
            "project": "deltashare",
        },
        "serverless": True,
    },
    {
        "pipeline_name": "customer-batch-dev",
        "target_catalog_name": "dltshr_dev",
        "target_schema_name": "03_gold",
        "configuration": {
            "pipelines.source_table": "dltshr_dev.02_silver.customers",
            "pipelines.keys": "customer_id",
            "pipelines.target_table": "customer_analytics",
            "pipelines.scd_type": "1",
        },
        "notifications_list": ["dev-team@example.com"],
        "tags": {
            "env": "dev",
            "team": "analytics",
        },
        "serverless": False,
    },
]
