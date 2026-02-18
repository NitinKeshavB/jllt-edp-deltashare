"""
Workflow Domain Database Connection Pool

Manages asyncpg connection pool for the workflow domain database (separate from logging DB).
Automatically runs migrations on initialization.

Schema Evolution:
-----------------
When adding/removing/renaming tables in schema.sql:
1. Update the schema.sql file with new DDL
2. Update DomainDBPool.EXPECTED_TABLES constant with new table names
3. For existing deployments, manually run migration or drop/recreate schema:
   DROP SCHEMA deltashare CASCADE;
   (then restart app to auto-create)

For production, consider implementing versioned migrations (e.g., Alembic, Flyway)
instead of this simple "all or nothing" approach.
"""

from typing import Optional

import asyncpg
from loguru import logger


class DomainDBPool:
    """Workflow domain database connection pool manager."""

    # Expected tables in deltashare schema (MVP v1.0)
    # Update this set when schema evolves (add/remove/rename tables)
    EXPECTED_TABLES = {
        "tenants",
        "tenant_regions",
        "projects",
        "requests",
        "share_packs",
        "recipients",
        "shares",
        "pipelines",
        "users",
        "ad_groups",
        "databricks_objects",
        "job_metrics",
        "project_costs",
        "sync_jobs",
        "notifications",
        "audit_trail",
    }

    def __init__(self, connection_string: str):
        """
        Initialize domain DB pool.

        Args:
            connection_string: PostgreSQL connection string for workflow domain database
        """
        self.connection_string = connection_string
        self.pool: Optional[asyncpg.Pool] = None
        self._pool_initialized = False

    async def initialize(self) -> None:
        """
        Initialize connection pool and run migrations.

        Creates the pool with retry logic and runs database migrations automatically.
        """
        if self._pool_initialized and self.pool is not None:
            logger.debug("Domain DB pool already initialized")
            return

        try:
            logger.info("Initializing workflow domain database pool")

            # Create connection pool
            # Using larger pool size than logging DB since this is the main domain database
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=2,  # Minimum connections
                max_size=10,  # Maximum connections
                command_timeout=60,  # Query timeout (60 seconds)
                timeout=15,  # Connection timeout (15 seconds)
                max_cached_statement_lifetime=0,  # Disable prepared statement caching (safer for DDL)
            )

            logger.info("Domain DB pool created successfully")

            # Validate pool connection
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    raise Exception("Pool validation query failed")

            logger.info("Domain DB pool validated")

            # Run migrations
            await self._run_migrations()

            self._pool_initialized = True
            logger.success("Workflow domain database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize domain DB pool: {e}", exc_info=True)
            if self.pool:
                await self.pool.close()
                self.pool = None
            raise

    async def _run_migrations(self) -> None:
        """
        Run database migrations (execute schema.sql if needed).

        Checks if deltashare schema and all expected tables exist. If not, creates them from schema.sql.
        """
        expected_count = len(self.EXPECTED_TABLES)

        try:
            async with self.pool.acquire() as conn:
                # Check if deltashare schema exists
                schema_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'deltashare')"
                )

                if schema_exists:
                    # Schema exists - get list of existing tables
                    existing_tables_result = await conn.fetch(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'deltashare'
                        ORDER BY table_name
                        """
                    )
                    existing_tables = {row["table_name"] for row in existing_tables_result}
                    table_count = len(existing_tables)

                    # Check if all expected tables exist
                    if existing_tables == self.EXPECTED_TABLES:
                        logger.info(
                            f"Deltashare schema and all {table_count} expected tables exist - running incremental migrations only"
                        )
                        from dbrx_api.workflow.db.migrations import run_incremental_migrations

                        await run_incremental_migrations(self.pool)
                        return

                    # Check for missing tables
                    missing_tables = self.EXPECTED_TABLES - existing_tables
                    extra_tables = existing_tables - self.EXPECTED_TABLES

                    if missing_tables and not extra_tables:
                        # Only missing tables (partial migration)
                        logger.error(
                            f"Deltashare schema exists but {len(missing_tables)} table(s) are missing: {missing_tables}. "
                            f"This indicates a partial or failed migration. "
                            f"Please manually drop the schema and restart: DROP SCHEMA deltashare CASCADE;"
                        )
                        raise RuntimeError(
                            f"Incomplete database schema: missing tables {missing_tables}. Manual intervention required."
                        )
                    elif extra_tables and not missing_tables:
                        # Only extra tables (schema evolved, need to drop old tables)
                        logger.error(
                            f"Deltashare schema contains {len(extra_tables)} unexpected table(s): {extra_tables}. "
                            f"Schema may have evolved. Expected tables: {self.EXPECTED_TABLES}. "
                            f"Please update DomainDBPool.EXPECTED_TABLES or manually clean up the schema."
                        )
                        raise RuntimeError(f"Unexpected tables in schema: {extra_tables}. Schema evolution required.")
                    elif missing_tables and extra_tables:
                        # Both missing and extra tables (complex evolution)
                        logger.error(
                            f"Schema mismatch detected. Missing: {missing_tables}, Extra: {extra_tables}. "
                            f"This may indicate schema evolution or migration failure. "
                            f"Please review schema.sql and DomainDBPool.EXPECTED_TABLES."
                        )
                        raise RuntimeError(
                            f"Schema evolution detected: missing {missing_tables}, extra {extra_tables}. "
                            f"Manual migration required."
                        )
                    elif table_count == 0:
                        # Schema exists but no tables - run migration
                        logger.warning("Deltashare schema exists but no tables found - running migrations")

                logger.info("Deltashare schema not found - running migrations")

                # Read schema.sql and execute
                from pathlib import Path

                schema_path = Path(__file__).parent / "schema.sql"
                if not schema_path.exists():
                    raise FileNotFoundError(f"schema.sql not found at {schema_path}")

                schema_sql = schema_path.read_text()

                # Execute schema creation
                await conn.execute(schema_sql)

                logger.success("Workflow database migrations completed")

                # Verify all expected tables exist
                existing_tables_result = await conn.fetch(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'deltashare'
                    ORDER BY table_name
                    """
                )
                existing_tables = {row["table_name"] for row in existing_tables_result}
                table_count = len(existing_tables)

                logger.info(f"Verification: {table_count} tables created in deltashare schema")

                if existing_tables != self.EXPECTED_TABLES:
                    missing_tables = self.EXPECTED_TABLES - existing_tables
                    extra_tables = existing_tables - self.EXPECTED_TABLES

                    logger.error(
                        f"Expected {expected_count} tables but found {table_count}. "
                        f"Missing: {missing_tables if missing_tables else 'None'}. "
                        f"Extra: {extra_tables if extra_tables else 'None'}. "
                        f"Existing: {sorted(existing_tables)}"
                    )
                    raise RuntimeError(
                        f"Migration incomplete: expected {expected_count} tables, found {table_count}. "
                        f"Missing: {missing_tables}, Extra: {extra_tables}"
                    )

                logger.success(f"All {expected_count} workflow tables verified successfully")

        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            raise

    async def close(self) -> None:
        """Close the connection pool gracefully."""
        if self.pool:
            logger.info("Closing workflow domain database pool")
            await self.pool.close()
            self.pool = None
            self._pool_initialized = False
            logger.info("Domain DB pool closed")

    def acquire(self):
        """
        Acquire a database connection from the pool.

        Returns async context manager that yields a connection.

        Usage:
            async with pool.acquire() as conn:
                result = await conn.fetchrow("SELECT * FROM ...")
        """
        if not self.pool:
            raise RuntimeError("Domain DB pool not initialized - call initialize() first")
        return self.pool.acquire()

    async def health_check(self) -> bool:
        """
        Check if database connection is healthy.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            if not self.pool:
                return False

            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"Domain DB health check failed: {e}")
            return False

    async def get_table_counts(self) -> dict:
        """
        Get row counts for all tables in deltashare schema.

        Useful for debugging and verification.

        Returns:
            Dict mapping table names to row counts
        """
        async with self.pool.acquire() as conn:
            tables = await conn.fetch(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'deltashare'
                ORDER BY table_name
                """
            )

            counts = {}
            for table in tables:
                table_name = table["table_name"]
                count = await conn.fetchval(f"SELECT COUNT(*) FROM deltashare.{table_name}")
                counts[table_name] = count

            return counts
