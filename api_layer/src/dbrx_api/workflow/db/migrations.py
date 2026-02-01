"""Database migrations for workflow domain.

This module handles schema initialization by executing the schema.sql file.
All DDL is stored in schema.sql for maintainability.
"""

from pathlib import Path

import asyncpg
from loguru import logger


async def run_migrations(pool: asyncpg.Pool) -> None:
    """Run database migrations to create schema and tables.

    This function:
    1. Reads the schema.sql file
    2. Executes it against the database
    3. Creates deltashare schema and all 16 tables if they don't exist

    All SQL uses CREATE TABLE IF NOT EXISTS, so it's safe to run multiple times.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool

    Raises
    ------
    FileNotFoundError
        If schema.sql file not found
    Exception
        If migration fails
    """
    # Get path to schema.sql (same directory as this file)
    schema_path = Path(__file__).parent / "schema.sql"

    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found: {schema_path}\n"
            "Expected location: api_layer/src/dbrx_api/workflow/db/schema.sql"
        )

    # Read schema SQL
    schema_sql = schema_path.read_text(encoding="utf-8")
    logger.info(f"Loaded schema from {schema_path}")

    # Execute migrations
    async with pool.acquire() as conn:
        try:
            await conn.execute(schema_sql)
            logger.info("✅ Workflow database migrations completed successfully")
            logger.info("   - Created deltashare schema")
            logger.info("   - Created 16 tables (11 SCD2 + 5 append-only)")
            logger.info("   - Created all indexes")

        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            raise


async def verify_schema(pool: asyncpg.Pool) -> dict:
    """Verify that all required tables exist.

    Returns
    -------
    dict
        {
            "schema_exists": bool,
            "tables": list[str],  # List of existing tables
            "missing_tables": list[str],
            "all_present": bool
        }
    """
    required_tables = [
        # SCD2 tables
        "tenants",
        "tenant_regions",
        "projects",
        "users",
        "ad_groups",
        "databricks_objects",
        "share_packs",
        "requests",
        "recipients",
        "shares",
        "pipelines",
        # Append-only tables
        "job_metrics",
        "project_costs",
        "sync_jobs",
        "notifications",
        "audit_trail",
    ]

    async with pool.acquire() as conn:
        # Check if schema exists
        schema_exists = await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1 FROM information_schema.schemata
                WHERE schema_name = 'deltashare'
            )
            """
        )

        if not schema_exists:
            return {
                "schema_exists": False,
                "tables": [],
                "missing_tables": required_tables,
                "all_present": False,
            }

        # Get list of existing tables
        existing_tables = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'deltashare'
            ORDER BY table_name
            """
        )
        existing_table_names = [row["table_name"] for row in existing_tables]

        # Find missing tables
        missing = [t for t in required_tables if t not in existing_table_names]

        return {
            "schema_exists": True,
            "tables": existing_table_names,
            "missing_tables": missing,
            "all_present": len(missing) == 0,
        }


async def get_table_counts(pool: asyncpg.Pool) -> dict:
    """Get row counts for all tables (useful for debugging).

    Returns
    -------
    dict
        {table_name: row_count, ...}
    """
    counts = {}

    async with pool.acquire() as conn:
        tables = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'deltashare'
            ORDER BY table_name
            """
        )

        for table_row in tables:
            table_name = table_row["table_name"]
            count = await conn.fetchval(f"SELECT COUNT(*) FROM deltashare.{table_name}")
            counts[table_name] = count

    return counts


if __name__ == "__main__":
    """
    Standalone script to run migrations.

    Usage:
        python -m dbrx_api.workflow.db.migrations
    """
    import asyncio

    from dbrx_api.settings import Settings

    async def main():
        settings = Settings()

        if not settings.domain_db_connection_string:
            logger.error("DOMAIN_DB_CONNECTION_STRING not set")
            return 1

        # Create pool
        pool = await asyncpg.create_pool(
            settings.domain_db_connection_string,
            min_size=1,
            max_size=2,
            command_timeout=60,
        )

        try:
            # Run migrations
            await run_migrations(pool)

            # Verify
            verification = await verify_schema(pool)
            if verification["all_present"]:
                logger.info("✅ All 16 tables present")
            else:
                logger.warning(f"⚠️  Missing tables: {verification['missing_tables']}")

            # Show counts
            counts = await get_table_counts(pool)
            logger.info("Table row counts:")
            for table, count in counts.items():
                logger.info(f"  - {table}: {count} rows")

        finally:
            await pool.close()

    asyncio.run(main())
