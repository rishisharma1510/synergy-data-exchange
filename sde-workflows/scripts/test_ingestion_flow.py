"""
test_ingestion_flow.py — Local test for INGESTION flow.

Downloads MDF from S3 → Copies to SQL Server share → Attaches with unique name → Reads → Writes to Iceberg

Usage:
    python scripts/test_ingestion_flow.py --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf --target-namespace rs_cdkdev_dclegend01_exposure_db
    python scripts/test_ingestion_flow.py --source-db AIRExposure_test --target-namespace rs_cdkdev_dclegend01_exposure_db --limit 100
"""

import argparse
import logging
import os
import shutil
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def generate_unique_db_name(prefix: str = "ingest_test") -> str:
    """Generate unique database name using timestamp ticks."""
    ticks = int(time.time() * 1000)  # milliseconds since epoch
    return f"{prefix}_{ticks}"


def download_from_s3(s3_uri: str, local_dir: str) -> str:
    """
    Download file from S3 to local directory.
    
    Returns local file path.
    """
    import boto3
    
    # Parse S3 URI
    # s3://bucket/key -> bucket, key
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    
    parts = s3_uri[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    
    filename = os.path.basename(key)
    local_path = os.path.join(local_dir, filename)
    
    os.makedirs(local_dir, exist_ok=True)
    
    logger.info("Downloading s3://%s/%s to %s", bucket, key, local_path)
    
    s3 = boto3.client("s3", verify=False)
    s3.download_file(bucket, key, local_path)
    
    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    logger.info("Downloaded: %.2f MB", file_size_mb)
    
    return local_path


def copy_to_sql_share(local_mdf: str, share_path: str, db_name: str) -> tuple[str, str]:
    """
    Copy MDF file to SQL Server share with unique name.
    
    Returns (local_path_for_attach, unc_path).
    """
    # Parse share config: local_path=unc_path
    # Example: C:\Program Files\...\DATA=\\10.193.57.161\sqlshare
    local_base, unc_base = share_path.split("=", 1)
    
    # Create new filename with db_name
    new_mdf_name = f"{db_name}.mdf"
    
    # UNC path for SQL Server
    unc_mdf_path = os.path.join(unc_base, new_mdf_name)
    
    # Local path SQL Server sees
    sql_local_path = os.path.join(local_base, new_mdf_name)
    
    logger.info("Copying MDF to share: %s", unc_mdf_path)
    shutil.copy2(local_mdf, unc_mdf_path)
    
    logger.info("SQL Server will see: %s", sql_local_path)
    
    return sql_local_path, unc_mdf_path


def attach_database(conn_str: str, db_name: str, mdf_path: str) -> None:
    """Attach MDF file to SQL Server as new database."""
    import pyodbc
    
    logger.info("Attaching database: %s from %s", db_name, mdf_path)
    
    # Connect to master
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    
    try:
        # Create database from MDF
        sql = f"""
        CREATE DATABASE [{db_name}]
        ON (FILENAME = N'{mdf_path}')
        FOR ATTACH_REBUILD_LOG
        """
        cursor.execute(sql)
        logger.info("Database attached: %s", db_name)
    except Exception as e:
        logger.error("Attach failed: %s", e)
        # Try simpler attach
        try:
            sql = f"""
            CREATE DATABASE [{db_name}]
            ON (FILENAME = N'{mdf_path}')
            FOR ATTACH
            """
            cursor.execute(sql)
            logger.info("Database attached (simple): %s", db_name)
        except Exception as e2:
            raise RuntimeError(f"Failed to attach database: {e2}") from e2
    finally:
        cursor.close()
        conn.close()


def drop_database(conn_str: str, db_name: str) -> None:
    """Drop database after testing."""
    import pyodbc
    
    logger.info("Dropping database: %s", db_name)
    
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    
    try:
        # Set single user to force disconnect
        cursor.execute(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        cursor.execute(f"DROP DATABASE [{db_name}]")
        logger.info("Database dropped: %s", db_name)
    except Exception as e:
        logger.warning("Drop failed (may already be dropped): %s", e)
    finally:
        cursor.close()
        conn.close()


def run_ingestion_from_existing_db(
    source_db: str,
    target_namespace: str,
    tables: list[str] | None = None,
    write_mode: str = "overwrite",
    row_limit: int | None = None,
):
    """
    Test ingestion from existing SQL Server database.
    """
    from src.core.sql_reader import SqlServerReader
    from src.core.iceberg_writer import IcebergWriter
    from src.core.column_mapper import ColumnMapper
    
    logger.info("=" * 60)
    logger.info("INGESTION TEST (from existing DB)")
    logger.info("=" * 60)
    logger.info("Source DB:        %s", source_db)
    logger.info("Target Namespace: %s", target_namespace)
    logger.info("Write Mode:       %s", write_mode)
    logger.info("Row Limit:        %s", row_limit or "None (all rows)")
    logger.info("=" * 60)
    
    cfg = load_config()
    cfg.sqlserver.database = source_db
    
    sql_reader = SqlServerReader(cfg.sqlserver, source_db)
    ice_writer = IcebergWriter(cfg.iceberg)
    
    try:
        logger.info("Connecting to SQL Server...")
        sql_reader.connect()
        
        if tables:
            tables_to_process = tables
        else:
            tables_to_process = sql_reader.list_tables()
            logger.info("Found %d tables in database", len(tables_to_process))
        
        if not tables_to_process:
            logger.warning("No tables found!")
            return
        
        logger.info("Tables: %s", tables_to_process)
        
        import pyarrow as pa
        
        total_rows = 0
        success_count = 0
        
        for table_name in tables_to_process:
            logger.info("-" * 40)
            logger.info("Processing: %s", table_name)
            
            try:
                row_count = sql_reader.get_row_count(table_name)
                logger.info("  Row count: %d", row_count)
                
                if row_count == 0:
                    logger.info("  Skipping empty table")
                    continue
                
                # Read data
                rows_read = 0
                batches = []
                
                for batch in sql_reader.stream_table(table_name, batch_size=10000):
                    batches.append(batch)
                    rows_read += batch.num_rows
                    
                    if row_limit and rows_read >= row_limit:
                        logger.info("  Limit reached: %d", rows_read)
                        break
                
                if not batches:
                    continue
                
                logger.info("  Read %d rows", rows_read)
                
                # Combine into table
                combined_table = pa.concat_tables([
                    pa.Table.from_batches([b]) for b in batches
                ])
                
                # Normalize column names to lowercase (Iceberg convention)
                combined_table = combined_table.rename_columns([
                    col.lower() for col in combined_table.column_names
                ])
                
                # Write to Iceberg
                iceberg_table = table_name.lower()
                logger.info("  Writing to: %s.%s", target_namespace, iceberg_table)
                
                ice_writer.create_namespace(target_namespace)
                
                ice_writer.create_table_if_not_exists(
                    namespace=target_namespace,
                    table_name=iceberg_table,
                    schema=combined_table.schema,
                )
                
                # Cast dataframe to match existing table schema
                try:
                    existing_table = ice_writer.catalog.load_table(f"{target_namespace}.{iceberg_table}")
                    target_schema = existing_table.schema().as_arrow()
                    source_cols = set(combined_table.column_names)
                    target_cols = set(f.name for f in target_schema)
                    
                    # Evolve schema if source has new columns
                    missing_cols = source_cols - target_cols
                    if missing_cols:
                        logger.info("  Evolving schema: adding %d columns: %s", len(missing_cols), missing_cols)
                        with existing_table.update_schema() as update:
                            for col_name in missing_cols:
                                arrow_field = combined_table.schema.field(col_name)
                                # Map Arrow types to Iceberg types
                                from pyiceberg.types import (
                                    StringType, LongType, IntegerType, DoubleType, 
                                    FloatType, BooleanType, TimestampType, DateType
                                )
                                arrow_type = arrow_field.type
                                if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                                    iceberg_type = StringType()
                                elif pa.types.is_int64(arrow_type):
                                    iceberg_type = LongType()
                                elif pa.types.is_int32(arrow_type):
                                    iceberg_type = IntegerType()
                                elif pa.types.is_float64(arrow_type):
                                    iceberg_type = DoubleType()
                                elif pa.types.is_float32(arrow_type):
                                    iceberg_type = FloatType()
                                elif pa.types.is_boolean(arrow_type):
                                    iceberg_type = BooleanType()
                                elif pa.types.is_timestamp(arrow_type):
                                    iceberg_type = TimestampType()
                                elif pa.types.is_date(arrow_type):
                                    iceberg_type = DateType()
                                else:
                                    iceberg_type = StringType()  # fallback
                                    logger.warning("  Unknown type %s for %s, using string", arrow_type, col_name)
                                
                                update.add_column(col_name, iceberg_type)
                        
                        # Reload table with updated schema
                        existing_table = ice_writer.catalog.load_table(f"{target_namespace}.{iceberg_table}")
                        target_schema = existing_table.schema().as_arrow()
                    
                    # Cast columns to match target types (only for columns that exist in target)
                    new_columns = []
                    for field in target_schema:
                        if field.name in source_cols:
                            source_col = combined_table.column(field.name)
                            if source_col.type != field.type:
                                logger.info("  Casting %s: %s → %s", field.name, source_col.type, field.type)
                                source_col = pa.compute.cast(source_col, field.type)
                            new_columns.append((field.name, source_col))
                        else:
                            # Column in target but not in source - add nulls
                            null_array = pa.nulls(combined_table.num_rows, type=field.type)
                            new_columns.append((field.name, null_array))
                    
                    combined_table = pa.table(
                        {name: col for name, col in new_columns},
                        schema=target_schema,
                    )
                except Exception as cast_error:
                    logger.warning("  Could not cast to target schema: %s", cast_error)
                    import traceback
                    traceback.print_exc()
                
                if write_mode == "overwrite":
                    ice_writer.overwrite(target_namespace, iceberg_table, combined_table)
                else:
                    ice_writer.append(target_namespace, iceberg_table, combined_table)
                
                logger.info("  ✓ Written %d rows", rows_read)
                total_rows += rows_read
                success_count += 1
                
            except Exception as e:
                logger.error("  ✗ Failed: %s", e)
                import traceback
                traceback.print_exc()
        
        logger.info("=" * 60)
        logger.info("INGESTION COMPLETE")
        logger.info("  Tables: %d / %d", success_count, len(tables_to_process))
        logger.info("  Rows:   %d", total_rows)
        logger.info("=" * 60)
        
    finally:
        sql_reader.close()


def run_ingestion_from_s3(
    s3_uri: str,
    target_namespace: str,
    data_share_path: str,
    conn_str: str,
    tables: list[str] | None = None,
    write_mode: str = "overwrite",
    row_limit: int | None = None,
    cleanup: bool = True,
):
    """
    Full ingestion test: S3 → MDF → Attach → Read → Iceberg
    """
    logger.info("=" * 60)
    logger.info("INGESTION TEST (from S3 MDF)")
    logger.info("=" * 60)
    logger.info("S3 URI:           %s", s3_uri)
    logger.info("Target Namespace: %s", target_namespace)
    logger.info("=" * 60)
    
    # Generate unique database name
    db_name = generate_unique_db_name()
    logger.info("Database name: %s", db_name)
    
    local_dir = "c:/temp/ingestion_test"
    local_mdf = None
    unc_mdf = None
    
    try:
        # 1. Download from S3
        local_mdf = download_from_s3(s3_uri, local_dir)
        
        # 2. Copy to SQL Server share
        sql_mdf_path, unc_mdf = copy_to_sql_share(local_mdf, data_share_path, db_name)
        
        # 3. Attach database
        attach_database(conn_str, db_name, sql_mdf_path)
        
        # 4. Run ingestion
        run_ingestion_from_existing_db(
            source_db=db_name,
            target_namespace=target_namespace,
            tables=tables,
            write_mode=write_mode,
            row_limit=row_limit,
        )
        
    finally:
        if cleanup:
            # Drop database
            try:
                drop_database(conn_str, db_name)
            except Exception as e:
                logger.warning("Cleanup drop failed: %s", e)
            
            # Remove UNC file
            if unc_mdf and os.path.exists(unc_mdf):
                try:
                    os.remove(unc_mdf)
                    logger.info("Removed: %s", unc_mdf)
                except Exception as e:
                    logger.warning("Cleanup file failed: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Test ingestion: S3/SQL Server → Iceberg")
    
    parser.add_argument(
        "--s3-uri",
        help="S3 URI to MDF file (e.g., s3://bucket/path/file.mdf)",
    )
    parser.add_argument(
        "--source-db",
        help="Existing SQL Server database to read from (alternative to --s3-uri)",
    )
    parser.add_argument(
        "--target-namespace",
        default="rs_cdkdev_dclegend01_exposure_db",
        help="Iceberg namespace to write to",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to ingest (default: all)",
    )
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Write mode (default: overwrite)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit rows per table",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't drop database after test",
    )
    
    args = parser.parse_args()
    
    if args.s3_uri:
        # Full test from S3
        cfg = load_config()
        
        # Build connection string
        conn_str = (
            f"DRIVER={{{cfg.sqlserver.driver}}};"
            f"SERVER={cfg.sqlserver.host},{cfg.sqlserver.port};"
            f"DATABASE=master;"
            f"UID={cfg.sqlserver.username};"
            f"PWD={cfg.sqlserver.password};"
            f"TrustServerCertificate={'yes' if cfg.sqlserver.trust_cert else 'no'};"
        )
        
        run_ingestion_from_s3(
            s3_uri=args.s3_uri,
            target_namespace=args.target_namespace,
            data_share_path=cfg.sqlserver.data_share_path,
            conn_str=conn_str,
            tables=args.tables,
            write_mode=args.write_mode,
            row_limit=args.limit,
            cleanup=not args.no_cleanup,
        )
    elif args.source_db:
        # Test from existing database
        run_ingestion_from_existing_db(
            source_db=args.source_db,
            target_namespace=args.target_namespace,
            tables=args.tables,
            write_mode=args.write_mode,
            row_limit=args.limit,
        )
    else:
        # Default: use AIRExposure_test
        run_ingestion_from_existing_db(
            source_db="AIRExposure_test",
            target_namespace=args.target_namespace,
            tables=args.tables,
            write_mode=args.write_mode,
            row_limit=args.limit,
        )


if __name__ == "__main__":
    main()
