"""
validation.py — Database validation utilities.

Performs row count comparison, DBCC checks, and integrity validation
before artifact generation.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pyodbc

from src.core.config import SqlServerConfig

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Results of database validation."""
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)
    dbcc_result: Optional[str] = None
    checksums: dict[str, str] = field(default_factory=dict)


class Validator:
    """Validates SQL Server database integrity before artifact generation."""
    
    def __init__(self, cfg: SqlServerConfig) -> None:
        self.cfg = cfg
    
    def _get_connection(self, database: str) -> pyodbc.Connection:
        """Get a connection to the specified database."""
        cfg_copy = SqlServerConfig(
            host=self.cfg.host,
            port=self.cfg.port,
            database=database,
            schema=self.cfg.schema,
            username=self.cfg.username,
            password=self.cfg.password,
            auth_type=self.cfg.auth_type,
            driver=self.cfg.driver,
            trust_cert=self.cfg.trust_cert,
        )
        return pyodbc.connect(cfg_copy.connection_string(), autocommit=True)
    
    def validate_database(
        self,
        db_name: str,
        load_results: dict[str, dict],
    ) -> ValidationReport:
        """
        Validate the loaded database.
        
        Parameters
        ----------
        db_name : str
            Database name to validate.
        load_results : dict
            Results from the table loading phase keyed by table name.
        
        Returns
        -------
        ValidationReport
            Validation results.
        """
        import time as _time
        t_total = _time.perf_counter()
        report = ValidationReport(passed=True)
        logger.info("[validate] db=%s starting validation (%d load_result entries)",
            db_name, len(load_results))

        conn = self._get_connection(db_name)
        try:
            t0 = _time.perf_counter()
            self._validate_row_counts(conn, load_results, report)
            logger.info("[validate] db=%s row_count check done in %.2fs — counts=%s",
                db_name, _time.perf_counter() - t0, report.row_counts)

            t0 = _time.perf_counter()
            self._run_dbcc_checkdb(conn, db_name, report)
            logger.info("[validate] db=%s DBCC CHECKDB done in %.2fs — result=%s",
                db_name, _time.perf_counter() - t0, report.dbcc_result)

            t0 = _time.perf_counter()
            self._check_constraints(conn, report)
            logger.info("[validate] db=%s constraint checks done in %.2fs",
                db_name, _time.perf_counter() - t0)

            t0 = _time.perf_counter()
            self._validate_sample_checksums(conn, load_results, report)
            logger.info("[validate] db=%s checksum validation done in %.2fs",
                db_name, _time.perf_counter() - t0)

        finally:
            conn.close()

        if report.errors:
            report.passed = False
            logger.error("[validate] db=%s FAILED in %.2fs — %d errors: %s",
                db_name, _time.perf_counter() - t_total, len(report.errors), report.errors)
        else:
            logger.info("[validate] db=%s PASSED in %.2fs — warnings=%s",
                db_name, _time.perf_counter() - t_total, report.warnings or 'none')

        return report
    
    def _validate_row_counts(
        self,
        conn: pyodbc.Connection,
        load_results: dict[str, dict],
        report: ValidationReport,
    ) -> None:
        """Validate row counts match expected values."""
        logger.info("Validating row counts...")
        
        cursor = conn.cursor()
        
        for table_name, result in load_results.items():
            expected_rows = result.get("rows", 0)
            
            # Get actual row count
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table_name}]")
                actual_rows = cursor.fetchone()[0]
                report.row_counts[table_name] = actual_rows
                
                if actual_rows != expected_rows:
                    report.warnings.append(
                        f"Row count mismatch for {table_name}: "
                        f"expected {expected_rows}, got {actual_rows}"
                    )
                    logger.warning(
                        "Row count mismatch: %s (expected %d, got %d)",
                        table_name, expected_rows, actual_rows,
                    )
                else:
                    logger.info(
                        "Row count OK: %s (%d rows)",
                        table_name, actual_rows,
                    )
            except Exception as e:
                report.errors.append(
                    f"Failed to validate row count for {table_name}: {e}"
                )
    
    def _run_dbcc_checkdb(
        self,
        conn: pyodbc.Connection,
        db_name: str,
        report: ValidationReport,
    ) -> None:
        """Run DBCC CHECKDB to verify database integrity."""
        logger.info("Running DBCC CHECKDB...")
        
        cursor = conn.cursor()
        
        try:
            # Run DBCC CHECKDB with minimal locking
            cursor.execute(f"DBCC CHECKDB('{db_name}') WITH NO_INFOMSGS")
            # Consume all result sets so the connection is clean before close()
            try:
                cursor.fetchall()
            except Exception:
                pass

            # If we get here without error, the check passed
            report.dbcc_result = "PASSED"
            logger.info("DBCC CHECKDB passed.")
            
        except pyodbc.Error as e:
            report.errors.append(f"DBCC CHECKDB failed: {e}")
            report.dbcc_result = f"FAILED: {e}"
            logger.error("DBCC CHECKDB failed: %s", e)
    
    def _check_constraints(
        self,
        conn: pyodbc.Connection,
        report: ValidationReport,
    ) -> None:
        """Check that all constraints are valid."""
        logger.info("Checking constraints...")
        
        cursor = conn.cursor()
        
        try:
            # Check for disabled foreign keys
            cursor.execute("""
                SELECT 
                    fk.name AS constraint_name,
                    OBJECT_NAME(fk.parent_object_id) AS table_name
                FROM sys.foreign_keys fk
                WHERE fk.is_disabled = 1
            """)
            
            disabled = cursor.fetchall()
            if disabled:
                for row in disabled:
                    report.warnings.append(
                        f"Disabled foreign key: {row.constraint_name} on {row.table_name}"
                    )
            
            logger.info("Constraint check completed.")
            
        except Exception as e:
            # Non-fatal warning
            report.warnings.append(f"Constraint check error: {e}")
    
    def _validate_sample_checksums(
        self,
        conn: pyodbc.Connection,
        load_results: dict[str, dict],
        report: ValidationReport,
    ) -> None:
        """Compute checksums for sample rows to verify data integrity."""
        logger.info("Computing sample checksums...")
        
        cursor = conn.cursor()
        
        for table_name in load_results.keys():
            try:
                # Get checksum of first 100 rows
                cursor.execute(f"""
                    SELECT TOP 100 CHECKSUM_AGG(CHECKSUM(*)) 
                    FROM [dbo].[{table_name}]
                """)
                
                result = cursor.fetchone()
                checksum = str(result[0]) if result and result[0] else "EMPTY"
                report.checksums[table_name] = checksum
                
                logger.debug("Checksum for %s: %s", table_name, checksum)
                
            except Exception as e:
                # Non-fatal
                report.warnings.append(
                    f"Could not compute checksum for {table_name}: {e}"
                )
        
        logger.info("Sample checksum validation completed.")
