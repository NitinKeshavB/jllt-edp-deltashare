-- ════════════════════════════════════════════════════════════════════════════
-- Delta Share Workflow - Database Schema
-- ════════════════════════════════════════════════════════════════════════════
--
-- This schema implements SCD Type 2 (Slowly Changing Dimension Type 2) pattern
-- for mutable entities to preserve full historical changes.
--
-- Tables: 16 total
--   - 11 SCD Type 2 tables (mutable entities with version history)
--   - 5 Append-only tables (immutable event logs)
--
-- Database: PostgreSQL 14+
-- Schema: deltashare
-- ════════════════════════════════════════════════════════════════════════════

-- Create schema
CREATE SCHEMA IF NOT EXISTS deltashare;

-- ════════════════════════════════════════════════════════════════════════════
-- SCD TYPE 2 MUTABLE ENTITIES
-- ════════════════════════════════════════════════════════════════════════════
--
-- SCD2 Column Pattern (on every mutable table):
--   record_id       UUID PRIMARY KEY      - Surrogate key (unique per version)
--   {entity}_id     UUID NOT NULL         - Business key (stable across versions)
--   effective_from  TIMESTAMPTZ           - When this version became active
--   effective_to    TIMESTAMPTZ           - When this version was superseded
--   is_current      BOOLEAN               - true for latest version
--   is_deleted      BOOLEAN               - true if soft-deleted
--   version         INT                   - Sequential version number
--   created_by      VARCHAR(255)          - Who/what created this version
--   change_reason   VARCHAR(500)          - Why this version was created
--
-- Query patterns:
--   Current state:  WHERE is_current = true AND is_deleted = false
--   Point-in-time:  WHERE effective_from <= $ts AND effective_to > $ts
--   Full history:   WHERE entity_id = $id ORDER BY version
-- ════════════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────────────
-- 1. TENANTS (Business Lines)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.tenants (
    -- Surrogate key (PK)
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Business key (stable across versions)
    tenant_id               UUID NOT NULL,

    -- Business data
    business_line_name      VARCHAR(255) NOT NULL,
    short_name              VARCHAR(50),
    executive_team          JSONB DEFAULT '[]'::jsonb,       -- ["group@jll.com", "user@jll.com"]
    configurator_ad_group   JSONB DEFAULT '[]'::jsonb,       -- ["config-group@jll.com"]
    owner                   VARCHAR(255),
    contact_email           VARCHAR(255),

    -- Soft delete flag
    is_deleted              BOOLEAN NOT NULL DEFAULT false,

    -- SCD2 columns
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_tenants_current
    ON deltashare.tenants(tenant_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_tenants_active
    ON deltashare.tenants(tenant_id) WHERE is_current = true AND is_deleted = false;
CREATE INDEX IF NOT EXISTS idx_tenants_name
    ON deltashare.tenants(business_line_name) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 2. TENANT REGIONS (Maps tenant + region -> workspace URL)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.tenant_regions (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_region_id        UUID NOT NULL,
    tenant_id               UUID NOT NULL,
    region                  VARCHAR(10) NOT NULL,             -- AM, EMEA
    workspace_url           VARCHAR(500) NOT NULL,

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tenant_regions_current
    ON deltashare.tenant_regions(tenant_id, region) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 3. PROJECTS
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.projects (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id              UUID NOT NULL,
    project_name            VARCHAR(255) NOT NULL,
    tenant_id               UUID NOT NULL,
    approver                JSONB DEFAULT '[]'::jsonb,        -- ["approver@jll.com", "ad-group"]
    configurator            JSONB DEFAULT '[]'::jsonb,        -- ["config@jll.com"]

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_current
    ON deltashare.projects(tenant_id, project_name) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 4. USERS (Synced from Azure AD)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.users (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                 UUID NOT NULL,
    email                   VARCHAR(255) NOT NULL,
    display_name            VARCHAR(255),
    job_title               VARCHAR(255),
    department              VARCHAR(255),
    is_active               BOOLEAN DEFAULT true,
    ad_object_id            VARCHAR(255),                     -- Azure AD object ID
    source                  VARCHAR(50) DEFAULT 'azure_ad',

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL DEFAULT 'ad_sync',
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email
    ON deltashare.users(email) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 5. AD GROUPS (Synced from Azure AD)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.ad_groups (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id                UUID NOT NULL,
    group_name              VARCHAR(255) NOT NULL,
    ad_object_id            VARCHAR(255),
    members                 JSONB DEFAULT '[]'::jsonb,        -- ["user1@jll.com", "user2@jll.com"]
    source                  VARCHAR(50) DEFAULT 'azure_ad',

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL DEFAULT 'ad_sync',
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ad_groups_name
    ON deltashare.ad_groups(group_name) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 6. DATABRICKS OBJECTS (Synced from Databricks workspaces)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.databricks_objects (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_id               UUID NOT NULL,
    workspace_url           VARCHAR(500) NOT NULL,
    full_name               VARCHAR(500) NOT NULL,            -- catalog.schema.table or catalog.schema
    object_type             VARCHAR(50) NOT NULL,             -- TABLE, VIEW, SCHEMA, CATALOG, NOTEBOOK, VOLUME
    catalog_name            VARCHAR(255),
    schema_name             VARCHAR(255),
    table_name              VARCHAR(255),
    owner                   VARCHAR(255),

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL DEFAULT 'dbrx_sync',
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dbrx_objects_current
    ON deltashare.databricks_objects(workspace_url, full_name) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 7. SHARE PACKS (The config bundle)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.share_packs (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    share_pack_id           UUID NOT NULL,
    share_pack_name         VARCHAR(255) NOT NULL,
    requested_by            VARCHAR(255) NOT NULL,
    strategy                VARCHAR(20) NOT NULL DEFAULT 'NEW',   -- NEW or UPDATE
    share_pack_status       VARCHAR(30) NOT NULL DEFAULT 'IN_PROGRESS',
    provisioning_status     TEXT DEFAULT '',
    error_message           TEXT DEFAULT '',
    config                  JSONB NOT NULL,                   -- Full YAML/Excel content as JSON
    file_format             VARCHAR(10) DEFAULT 'yaml',       -- yaml or xlsx
    original_filename       VARCHAR(500),
    tenant_id               UUID,
    project_id              UUID,

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_share_packs_current
    ON deltashare.share_packs(share_pack_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_share_packs_active
    ON deltashare.share_packs(share_pack_id) WHERE is_current = true AND is_deleted = false;
CREATE INDEX IF NOT EXISTS idx_share_packs_status
    ON deltashare.share_packs(share_pack_status) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_share_packs_tenant
    ON deltashare.share_packs(tenant_id) WHERE is_current = true;

-- ────────────────────────────────────────────────────────────────────────────
-- 8. REQUESTS (Tracks approval workflow)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.requests (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id              UUID NOT NULL,
    project_id              UUID NOT NULL,
    share_pack_id           UUID NOT NULL,
    requested_by            VARCHAR(255) NOT NULL,
    approved_by             VARCHAR(255),
    assigned_to             VARCHAR(255),
    service_now_ticket      VARCHAR(100),
    ado_ticket              VARCHAR(100),
    request_description     TEXT NOT NULL,
    status                  VARCHAR(30) NOT NULL DEFAULT 'IN_PROGRESS',
    request_type            VARCHAR(20) NOT NULL DEFAULT 'NEW',
    approver_status         VARCHAR(30) DEFAULT 'approved',   -- approved|declined|request_more_info
    assigned_datetime       TIMESTAMPTZ,
    approved_datetime       TIMESTAMPTZ,

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_requests_current
    ON deltashare.requests(request_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_requests_project
    ON deltashare.requests(project_id) WHERE is_current = true;

-- ────────────────────────────────────────────────────────────────────────────
-- 9. RECIPIENTS (Provisioned recipients)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.recipients (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_id            UUID NOT NULL,
    share_pack_id           UUID NOT NULL,
    recipient_name          VARCHAR(255) NOT NULL,
    recipient_contact_email VARCHAR(255),                     -- Contact person email
    recipient_type          VARCHAR(10) NOT NULL,             -- D2D or D2O
    recipient_databricks_org VARCHAR(500),                    -- Databricks org/metastore ID
    recipient_databricks_id VARCHAR(255),                     -- Set after provisioning (via regular UPDATE)
    description             TEXT,
    client_ip_addresses     JSONB DEFAULT '[]'::jsonb,
    token_expiry_days       INT,
    token_rotation          BOOLEAN DEFAULT false,

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_recipients_current
    ON deltashare.recipients(recipient_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_recipients_active
    ON deltashare.recipients(recipient_id) WHERE is_current = true AND is_deleted = false;
CREATE UNIQUE INDEX IF NOT EXISTS idx_recipients_name
    ON deltashare.recipients(recipient_name) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 10. SHARES (Provisioned shares)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.shares (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    share_id                UUID NOT NULL,
    share_pack_id           UUID NOT NULL,
    share_name              VARCHAR(255) NOT NULL,
    databricks_share_id     VARCHAR(255),                     -- Set after provisioning (via regular UPDATE)
    share_assets            JSONB NOT NULL DEFAULT '[]'::jsonb,  -- List of asset strings from YAML
    recipients              JSONB NOT NULL DEFAULT '[]'::jsonb,  -- List of recipient names

    -- Delta Share target config
    ext_catalog_name        VARCHAR(255),
    ext_schema_name         VARCHAR(255),
    prefix_assetname        VARCHAR(100),
    share_tags              JSONB DEFAULT '[]'::jsonb,

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_shares_current
    ON deltashare.shares(share_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_shares_active
    ON deltashare.shares(share_id) WHERE is_current = true AND is_deleted = false;
CREATE INDEX IF NOT EXISTS idx_shares_databricks_id
    ON deltashare.shares(databricks_share_id) WHERE is_current = true AND is_deleted = false;

-- ────────────────────────────────────────────────────────────────────────────
-- 11. PIPELINES (Provisioned pipelines - one row per asset schedule)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.pipelines (
    record_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id             UUID NOT NULL,
    share_pack_id           UUID NOT NULL,
    share_id                UUID NOT NULL,                    -- Parent share
    pipeline_name           VARCHAR(255) NOT NULL,            -- {name_prefix}_{asset_name}
    databricks_pipeline_id  VARCHAR(255),                     -- DLT pipeline ID (set via regular UPDATE)
    databricks_job_id       VARCHAR(255),                     -- Job ID for schedule (set via regular UPDATE)
    name_prefix             VARCHAR(255),
    asset_name              VARCHAR(500) NOT NULL,            -- Source asset this pipeline syncs
    source_table            VARCHAR(500) NOT NULL,            -- Fully qualified source
    target_table            VARCHAR(500) NOT NULL,            -- {ext_catalog}.{ext_schema}.{prefix}_{asset}
    scd_type                VARCHAR(20) DEFAULT '2',          -- 1, 2, or full_refresh
    key_columns             VARCHAR(500),                     -- Comma-separated, required for SCD2
    schedule_type           VARCHAR(20) NOT NULL,             -- CRON or CONTINUOUS
    cron_expression         VARCHAR(100),
    cron_timezone           VARCHAR(100) DEFAULT 'UTC',
    notification_list       JSONB DEFAULT '[]'::jsonb,
    tags                    JSONB DEFAULT '{}'::jsonb,
    serverless              BOOLEAN DEFAULT false,

    is_deleted              BOOLEAN NOT NULL DEFAULT false,
    effective_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_to            TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current              BOOLEAN NOT NULL DEFAULT true,
    version                 INT NOT NULL DEFAULT 1,
    created_by              VARCHAR(255) NOT NULL,
    change_reason           VARCHAR(500) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_pipelines_current
    ON deltashare.pipelines(pipeline_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_pipelines_active
    ON deltashare.pipelines(pipeline_id) WHERE is_current = true AND is_deleted = false;
CREATE INDEX IF NOT EXISTS idx_pipelines_share
    ON deltashare.pipelines(share_id) WHERE is_current = true;
CREATE INDEX IF NOT EXISTS idx_pipelines_databricks_pipeline_id
    ON deltashare.pipelines(databricks_pipeline_id) WHERE is_current = true AND is_deleted = false;
CREATE INDEX IF NOT EXISTS idx_pipelines_databricks_job_id
    ON deltashare.pipelines(databricks_job_id) WHERE is_current = true AND is_deleted = false;

-- ════════════════════════════════════════════════════════════════════════════
-- APPEND-ONLY TABLES (Immutable event logs)
-- ════════════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────────────
-- 12. JOB METRICS (Pipeline execution metrics)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.job_metrics (
    metrics_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id             UUID,                             -- FK to deltashare.pipelines.pipeline_id
    databricks_pipeline_id  VARCHAR(255) NOT NULL,
    pipeline_name           VARCHAR(255) NOT NULL,
    start_time              TIMESTAMPTZ,
    end_time                TIMESTAMPTZ,
    duration_seconds        DOUBLE PRECISION,
    status                  VARCHAR(50) NOT NULL,
    run_id                  VARCHAR(255),
    error_message           TEXT,
    collected_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_pipeline ON deltashare.job_metrics(databricks_pipeline_id);
CREATE INDEX IF NOT EXISTS idx_metrics_time ON deltashare.job_metrics(collected_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_run_id ON deltashare.job_metrics(run_id);

-- ────────────────────────────────────────────────────────────────────────────
-- 13. PROJECT COSTS (Azure and Databricks cost tracking)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.project_costs (
    cost_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id              UUID NOT NULL,
    tenant_id               UUID NOT NULL,
    period_start            DATE NOT NULL,
    period_end              DATE NOT NULL,
    period_type             VARCHAR(20) NOT NULL DEFAULT 'weekly',  -- weekly or monthly
    databricks_cost         DOUBLE PRECISION DEFAULT 0,
    azure_cost              DOUBLE PRECISION DEFAULT 0,
    network_cost            DOUBLE PRECISION DEFAULT 0,
    io_cost                 DOUBLE PRECISION DEFAULT 0,
    total_cost              DOUBLE PRECISION GENERATED ALWAYS AS
                            (databricks_cost + azure_cost + network_cost + io_cost) STORED,
    currency                VARCHAR(10) DEFAULT 'USD',
    collected_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_costs_project ON deltashare.project_costs(project_id);
CREATE INDEX IF NOT EXISTS idx_costs_period ON deltashare.project_costs(period_start, period_end);

-- ────────────────────────────────────────────────────────────────────────────
-- 14. SYNC JOBS (Tracks every sync execution)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.sync_jobs (
    sync_job_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_type               VARCHAR(50) NOT NULL,             -- AD_USERS, AD_GROUPS, DATABRICKS_OBJECTS, JOB_METRICS, PROJECT_COSTS
    workspace_url           VARCHAR(500),                     -- Null for AD syncs
    status                  VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING, COMPLETED, FAILED, INTERRUPTED
    started_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    records_processed       INT DEFAULT 0,
    records_created         INT DEFAULT 0,
    records_updated         INT DEFAULT 0,
    records_failed          INT DEFAULT 0,
    error_message           TEXT DEFAULT '',
    details                 JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_sync_type_time ON deltashare.sync_jobs(sync_type, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_status ON deltashare.sync_jobs(status) WHERE status IN ('FAILED', 'INTERRUPTED');

-- ────────────────────────────────────────────────────────────────────────────
-- 15. NOTIFICATIONS (Outbound email notifications)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.notifications (
    notification_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_type       VARCHAR(50) NOT NULL,             -- PROVISION_SUCCESS, PROVISION_FAILURE, SYNC_FAILURE, etc.
    recipient_email         VARCHAR(255) NOT NULL,
    subject                 VARCHAR(500) NOT NULL,
    body                    TEXT NOT NULL,
    related_entity_type     VARCHAR(50),                      -- share_pack, sync_job, pipeline, etc.
    related_entity_id       UUID,
    status                  VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    sent_at                 TIMESTAMPTZ,
    error_message           TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notif_status ON deltashare.notifications(status);

-- ────────────────────────────────────────────────────────────────────────────
-- 16. AUDIT TRAIL (Every action on every entity)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS deltashare.audit_trail (
    audit_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type             VARCHAR(50) NOT NULL,             -- tenant, project, share_pack, share, recipient, pipeline
    entity_id               UUID NOT NULL,
    action                  VARCHAR(50) NOT NULL,             -- CREATED, UPDATED, DELETED, STATUS_CHANGED, PROVISIONED, RECREATED
    performed_by            VARCHAR(255) NOT NULL,
    old_values              JSONB,
    new_values              JSONB,
    timestamp               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_entity ON deltashare.audit_trail(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_time ON deltashare.audit_trail(timestamp DESC);

-- ════════════════════════════════════════════════════════════════════════════
-- END OF SCHEMA
-- ════════════════════════════════════════════════════════════════════════════
