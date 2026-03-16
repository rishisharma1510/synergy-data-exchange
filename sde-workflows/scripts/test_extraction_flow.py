#!/usr/bin/env python3
"""
test_extraction_flow.py — Full extraction test script.

Tests the complete extraction pipeline:
1. Create ephemeral database
2. Extract data from all Iceberg tables (with row limit)
3. Generate MDF artifact
4. Upload to S3 bucket

Usage:
    python scripts/test_extraction_flow.py --limit 1000 --bucket rks-sd-virusscan
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import replace
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("test_extraction")


def create_database_direct(db_mgr, db_name: str, recreate: bool = False) -> str:
    """Create a database with a specific name (not tenant_id + run_id).
    
    Handles the case where database was detached but files still exist.
    """
    import pyodbc
    from src.core.config import SqlServerConfig
    
    logger.info("Creating database: %s", db_name)
    
    cfg = db_mgr.cfg
    cfg_copy = SqlServerConfig(
        host=cfg.host,
        port=cfg.port,
        database="master",
        schema=cfg.schema,
        username=cfg.username,
        password=cfg.password,
        auth_type=cfg.auth_type,
        driver=cfg.driver,
        trust_cert=cfg.trust_cert,
    )
    conn = pyodbc.connect(cfg_copy.connection_string(), autocommit=True)
    try:
        cursor = conn.cursor()
        
        # Check if database already exists
        cursor.execute(
            "SELECT 1 FROM sys.databases WHERE name = ?",
            db_name,
        )
        if cursor.fetchone():
            if recreate:
                logger.warning("Database %s already exists — dropping first.", db_name)
                cursor.execute(f"""
                    ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db_name}];
                """)
            else:
                logger.info("Database %s already exists — reusing.", db_name)
                return db_name
        
        # Get default data path from master
        cursor.execute("SELECT physical_name FROM sys.master_files WHERE database_id = 1 AND file_id = 1")
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("Could not determine SQL Server data directory")
        master_path: str = row[0]
        data_dir = master_path.rsplit("\\", 1)[0]
        logger.info("SQL Server data dir: %s", data_dir)
        
        mdf_path = f"{data_dir}\\{db_name}.mdf"
        ldf_path = f"{data_dir}\\{db_name}_log.ldf"
        
        # Try to create database
        try:
            cursor.execute(f"CREATE DATABASE [{db_name}];")
            cursor.execute(f"ALTER DATABASE [{db_name}] SET RECOVERY SIMPLE;")
            logger.info("Database %s created successfully.", db_name)
        except pyodbc.ProgrammingError as e:
            if "already exists" in str(e):
                # Files exist from a detached database - try to attach
                logger.warning("MDF file exists - attempting to attach orphaned database...")
                try:
                    cursor.execute(f"""
                        CREATE DATABASE [{db_name}]
                        ON (FILENAME = '{mdf_path}'),
                           (FILENAME = '{ldf_path}')
                        FOR ATTACH;
                    """)
                    logger.info("Attached existing database files.")
                    
                    if recreate:
                        # Now drop and recreate
                        cursor.execute(f"""
                            ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                            DROP DATABASE [{db_name}];
                        """)
                        cursor.execute(f"CREATE DATABASE [{db_name}];")
                        cursor.execute(f"ALTER DATABASE [{db_name}] SET RECOVERY SIMPLE;")
                        logger.info("Database %s recreated successfully.", db_name)
                    else:
                        logger.info("Reusing attached database.")
                except pyodbc.ProgrammingError as attach_err:
                    logger.error("Failed to attach: %s", attach_err)
                    raise RuntimeError(f"Cannot create or attach database - file conflict. Try --recreate-db or delete {mdf_path} manually.") from attach_err
            else:
                raise
        
        return db_name
        
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run full extraction flow test"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max rows to extract per table (default: 1000)",
    )
    parser.add_argument(
        "--bucket",
        default="rks-sd-virusscan",
        help="S3 bucket for upload (default: rks-sd-virusscan)",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Database name (default: from .env SQLSERVER_DATABASE)",
    )
    parser.add_argument(
        "--artifact-type",
        choices=["bak", "mdf", "both"],
        default="mdf",
        help="Artifact type to generate (default: mdf)",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload (for testing locally)",
    )
    parser.add_argument(
        "--skip-mdf",
        action="store_true",
        help="Skip MDF/BAK artifact generation (for remote testing)",
    )
    parser.add_argument(
        "--recreate-db",
        action="store_true",
        help="Drop and recreate database if it exists",
    )
    parser.add_argument(
        "--keep-database",
        action="store_true",
        help="Keep database after extraction (for inspection)",
    )
    args = parser.parse_args()

    # Load environment
    load_dotenv(args.env_file)

    # Import modules after env
    from src.core.config import load_config, SqlServerConfig, IcebergConfig
    from src.core.iceberg_reader import IcebergReader
    from src.core.sql_writer import SqlServerWriter
    from src.fargate.database_manager import DatabaseManager
    from src.fargate.artifact_generator import ArtifactGenerator
    from src.fargate.s3_uploader import S3Uploader

    cfg = load_config()
    
    # Use database from .env if not specified
    db_name = args.database or cfg.sqlserver.database
    
    logger.info("=" * 60)
    logger.info("EXTRACTION FLOW TEST")
    logger.info("=" * 60)
    logger.info("Row limit per table: %d", args.limit)
    logger.info("Target database: %s", db_name)
    logger.info("Artifact type: %s", args.artifact_type)
    logger.info("S3 bucket: %s", args.bucket)
    logger.info("SQL Server: %s:%s", cfg.sqlserver.host, cfg.sqlserver.port)
    logger.info("Iceberg catalog: %s", cfg.iceberg.catalog_type)
    logger.info("Iceberg namespace: %s", cfg.iceberg.namespace)
    logger.info("=" * 60)

    db_mgr = DatabaseManager(cfg.sqlserver)

    try:
        # Step 1: Create database
        logger.info("")
        logger.info("STEP 1: Creating database...")
        create_database_direct(db_mgr, db_name, recreate=args.recreate_db)
        logger.info("Database created: %s", db_name)

        # Step 2: Discover Iceberg tables
        logger.info("")
        logger.info("STEP 2: Discovering Iceberg tables...")
        reader = IcebergReader(cfg.iceberg)
        tables = reader.list_tables()
        logger.info("Found %d tables: %s", len(tables), tables)

        # Step 3: Extract data from each table
        logger.info("")
        logger.info("STEP 3: Extracting data from Iceberg tables...")
        
        extraction_results = []
        total_rows = 0
        
        for idx, full_table_name in enumerate(tables, 1):
            # Parse namespace.table
            if "." in full_table_name:
                namespace, table_name = full_table_name.split(".", 1)
            else:
                namespace = cfg.iceberg.namespace
                table_name = full_table_name
            
            logger.info("")
            logger.info("[%d/%d] Extracting: %s.%s", idx, len(tables), namespace, table_name)
            
            try:
                # Reset reader for this table
                reader_cfg_copy = replace(cfg.iceberg, namespace=namespace, table=table_name)
                table_reader = IcebergReader(reader_cfg_copy)
                
                # Get schema
                schema_fields = table_reader.schema_fields()
                arrow_schema = table_reader.get_arrow_schema()
                logger.info("  Schema: %d columns", len(schema_fields))
                
                # Create SQL writer for this table
                writer_cfg = replace(cfg.sqlserver, database=db_name, table=table_name)
                writer = SqlServerWriter(writer_cfg)
                
                # Read data with limit
                arrow_table = table_reader.read(mode="full", limit=args.limit)
                row_count = arrow_table.num_rows
                
                if row_count > 0:
                    # Create table and write data
                    writer.write(arrow_table)
                    logger.info("  Extracted: %d rows", row_count)
                else:
                    # Create empty table from schema
                    writer.create_table(arrow_schema)
                    logger.info("  Table created (empty)")
                
                total_rows += row_count
                
                extraction_results.append({
                    "table": f"{namespace}.{table_name}",
                    "rows": row_count,
                    "status": "success",
                })
                
            except Exception as e:
                logger.error("  Failed to extract %s: %s", table_name, e)
                extraction_results.append({
                    "table": f"{namespace}.{table_name}",
                    "rows": 0,
                    "status": "failed",
                    "error": str(e),
                })

        logger.info("")
        logger.info("Extraction complete: %d total rows from %d tables", total_rows, len(tables))

        # Step 4: Generate artifact
        artifacts = []
        if args.skip_mdf:
            logger.info("")
            logger.info("STEP 4: Skipping artifact generation (--skip-mdf)")
        else:
            logger.info("")
            logger.info("STEP 4: Generating %s artifact...", args.artifact_type.upper())
            
            artifact_dir = Path("./artifacts")
            artifact_dir.mkdir(exist_ok=True)
            
            artifact_gen = ArtifactGenerator(cfg.sqlserver)
            
            if args.artifact_type in ("mdf", "both"):
                mdf_result = artifact_gen.generate_mdf(db_name, str(artifact_dir))
                artifacts.extend(mdf_result)
                for art in mdf_result:
                    logger.info("  Generated: %s (%d bytes)", art.file_path, art.size_bytes)
            
            if args.artifact_type in ("bak", "both"):
                bak_result = artifact_gen.generate_bak(db_name, str(artifact_dir))
                artifacts.append(bak_result)
                logger.info("  Generated: %s (%d bytes)", bak_result.file_path, bak_result.size_bytes)

        # Step 5: Upload to S3
        if not args.skip_upload:
            logger.info("")
            logger.info("STEP 5: Uploading to S3 bucket: %s", args.bucket)
            
            uploader = S3Uploader("test")
            prefix = f"extraction-test/{db_name}/"
            
            upload_results = uploader.upload_artifacts(
                artifacts=artifacts,
                tenant_bucket=args.bucket,
                prefix=prefix,
            )
            
            for result in upload_results:
                logger.info("  Uploaded: %s (%d bytes)", result.s3_uri, result.size_bytes)
        else:
            logger.info("")
            logger.info("STEP 5: Skipping S3 upload (--skip-upload)")

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("EXTRACTION TEST COMPLETE")
        logger.info("=" * 60)
        logger.info("Database: %s", db_name)
        logger.info("Total tables: %d", len(tables))
        logger.info("Total rows: %d", total_rows)
        logger.info("Artifacts: %s", [Path(a.file_path).name for a in artifacts])
        
        success_count = sum(1 for r in extraction_results if r["status"] == "success")
        failed_count = len(extraction_results) - success_count
        logger.info("Success: %d / Failed: %d", success_count, failed_count)
        
        if failed_count > 0:
            logger.warning("Failed tables:")
            for r in extraction_results:
                if r["status"] == "failed":
                    logger.warning("  - %s: %s", r["table"], r.get("error", "unknown"))

    except Exception as e:
        logger.exception("Extraction test failed: %s", e)
        sys.exit(1)

    finally:
        # Cleanup
        if not args.keep_database:
            try:
                logger.info("Cleaning up database: %s", db_name)
                db_mgr.drop_database(db_name)
            except Exception as e:
                logger.warning("Failed to cleanup database: %s", e)
        else:
            logger.info("Keeping database for inspection: %s", db_name)


if __name__ == "__main__":
    main()
