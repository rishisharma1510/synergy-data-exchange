"""
database_manager.py — SQL Server database lifecycle management.

Handles CREATE/DROP DATABASE operations for ephemeral extraction databases.
"""

import logging
import time
from typing import Optional

import pyodbc

from src.core.config import SqlServerConfig

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages ephemeral SQL Server database lifecycle."""
    
    def __init__(self, cfg: SqlServerConfig) -> None:
        self.cfg = cfg
        self._conn: Optional[pyodbc.Connection] = None
    
    def _get_connection(self, database: str = "master") -> pyodbc.Connection:
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
    
    def create_database(self, tenant_id: str, run_id: str) -> str:
        """
        Create a new database for the extraction run.
        
        Parameters
        ----------
        tenant_id : str
            Tenant identifier.
        run_id : str
            Unique run identifier.
        
        Returns
        -------
        str
            The created database name.
        """
        # Generate database name (max 128 chars for SQL Server)
        run_id_short = run_id[:8] if len(run_id) > 8 else run_id
        db_name = f"tenant_{tenant_id}_{run_id_short}"
        db_name = db_name[:128]  # Truncate to SQL Server limit
        
        import time as _time
        t_total = _time.perf_counter()
        logger.info("[create_database] db=%s host=%s", db_name, self.cfg.host)

        conn = self._get_connection("master")
        try:
            cursor = conn.cursor()

            t0 = _time.perf_counter()
            cursor.execute("SELECT 1 FROM sys.databases WHERE name = ?", db_name)
            if cursor.fetchone():
                logger.warning("[create_database] db=%s already exists — dropping first.", db_name)
                self.drop_database(db_name)

            logger.info("[create_database] db=%s running CREATE DATABASE...", db_name)
            t0 = _time.perf_counter()
            cursor.execute(f"CREATE DATABASE [{db_name}]")
            logger.info("[create_database] db=%s CREATE done in %.2fs", db_name, _time.perf_counter() - t0)

            t0 = _time.perf_counter()
            cursor.execute(f"ALTER DATABASE [{db_name}] SET RECOVERY SIMPLE")
            logger.info("[create_database] db=%s RECOVERY SIMPLE set in %.2fs", db_name, _time.perf_counter() - t0)

            logger.info("[create_database] db=%s ready — total %.2fs", db_name, _time.perf_counter() - t_total)
            return db_name

        finally:
            conn.close()
    
    def drop_database(self, db_name: str) -> None:
        """
        Drop a database.
        
        Parameters
        ----------
        db_name : str
            Name of the database to drop.
        """
        import time as _time
        t_total = _time.perf_counter()
        logger.info("[drop_database] db=%s", db_name)

        conn = self._get_connection("master")
        try:
            cursor = conn.cursor()

            logger.info("[drop_database] db=%s setting SINGLE_USER...", db_name)
            t0 = _time.perf_counter()
            cursor.execute(f"""
                IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{db_name}')
                    ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
            """)
            try:
                cursor.fetchall()
            except Exception:
                pass
            logger.info("[drop_database] db=%s SINGLE_USER in %.2fs", db_name, _time.perf_counter() - t0)

            logger.info("[drop_database] db=%s running DROP DATABASE...", db_name)
            t0 = _time.perf_counter()
            cursor.execute(f"""
                IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{db_name}')
                    DROP DATABASE [{db_name}]
            """)
            try:
                cursor.fetchall()
            except Exception:
                pass
            logger.info("[drop_database] db=%s DROP done in %.2fs", db_name, _time.perf_counter() - t0)

            logger.info("[drop_database] db=%s complete — total %.2fs", db_name, _time.perf_counter() - t_total)

        finally:
            conn.close()
    
    def wait_for_ready(self, timeout_seconds: int = 120) -> bool:
        """
        Wait for SQL Server to be responsive.
        
        Parameters
        ----------
        timeout_seconds : int
            Maximum time to wait.
        
        Returns
        -------
        bool
            True if SQL Server is ready, False if timeout.
        """
        logger.info("Waiting for SQL Server to be ready (timeout: %ds)...", timeout_seconds)
        
        start_time = time.time()
        last_error = None
        
        while time.time() - start_time < timeout_seconds:
            try:
                conn = self._get_connection("master")
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                conn.close()
                logger.info("SQL Server is ready.")
                return True
            except Exception as e:
                last_error = e
                time.sleep(2)
        
        logger.error(
            "SQL Server not ready after %ds. Last error: %s",
            timeout_seconds, last_error,
        )
        return False
    
    def create_tables_from_plan(self, db_name: str, plan: list[dict]) -> None:
        """
        Create tables in the target database from an execution plan.
        
        Parameters
        ----------
        db_name : str
            Target database name.
        plan : list[dict]
            Execution plan with table definitions.
        """
        logger.info("Creating %d tables in database %s...", len(plan), db_name)
        
        conn = self._get_connection(db_name)
        try:
            cursor = conn.cursor()
            
            for table_plan in plan:
                table_name = table_plan.get("name", "unknown")
                columns = table_plan.get("columns", [])
                
                if not columns:
                    logger.warning("No columns defined for table %s — skipping.", table_name)
                    continue
                
                # Build CREATE TABLE statement
                col_defs = []
                for col in columns:
                    col_name = col.get("name")
                    col_type = col.get("sql_type", "NVARCHAR(MAX)")
                    nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
                    col_defs.append(f"    [{col_name}] {col_type} {nullable}")
                
                ddl = f"""
                    CREATE TABLE [dbo].[{table_name}] (
                        {','.join(col_defs)}
                    );
                """
                
                cursor.execute(ddl)
                logger.info("Created table: %s", table_name)
            
            conn.commit()
            logger.info("All tables created successfully.")
            
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Restore operations (for ingestion flow)
    # ------------------------------------------------------------------

    def restore_database(
        self,
        bak_file_path: str,
        target_db_name: str,
        data_dir: str = "/data/mdf",
    ) -> str:
        """
        Restore a database from a .bak file.
        
        Parameters
        ----------
        bak_file_path : str
            Full path to the .bak file.
        target_db_name : str
            Name for the restored database.
        data_dir : str
            Directory for MDF/LDF files.
        
        Returns
        -------
        str
            Name of the restored database.
        """
        import os
        from pathlib import Path
        
        logger.info("Restoring database %s from %s", target_db_name, bak_file_path)
        
        conn = self._get_connection("master")
        try:
            cursor = conn.cursor()
            
            # Drop existing database if it exists
            cursor.execute(
                "SELECT 1 FROM sys.databases WHERE name = ?",
                target_db_name,
            )
            if cursor.fetchone():
                logger.warning("Database %s exists — dropping first.", target_db_name)
                self.drop_database(target_db_name)
            
            # Get logical file names from backup
            cursor.execute(f"RESTORE FILELISTONLY FROM DISK = N'{bak_file_path}'")
            files = cursor.fetchall()
            
            if not files:
                raise ValueError(f"No files found in backup: {bak_file_path}")
            
            # Build MOVE clauses
            data_path = Path(data_dir)
            data_path.mkdir(parents=True, exist_ok=True)
            
            move_clauses = []
            for file_info in files:
                logical_name = file_info[0]
                file_type = file_info[2]  # 'D' for data, 'L' for log
                
                if file_type == 'D':
                    target_path = data_path / f"{target_db_name}.mdf"
                else:
                    target_path = data_path / f"{target_db_name}_log.ldf"
                
                move_clauses.append(f"MOVE N'{logical_name}' TO N'{target_path}'")
            
            # Execute RESTORE
            restore_sql = f"""
                RESTORE DATABASE [{target_db_name}]
                FROM DISK = N'{bak_file_path}'
                WITH 
                    {', '.join(move_clauses)},
                    REPLACE,
                    RECOVERY
            """
            
            cursor.execute(restore_sql)
            logger.info("Database %s restored successfully.", target_db_name)
            
            return target_db_name
            
        finally:
            conn.close()

    def get_database_state(self, db_name: str) -> dict:
        """
        Get the state of a database.
        
        Parameters
        ----------
        db_name : str
            Database name.
        
        Returns
        -------
        dict
            Database state information.
        """
        conn = self._get_connection("master")
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    state_desc,
                    user_access_desc,
                    is_read_only,
                    recovery_model_desc
                FROM sys.databases
                WHERE name = ?
            """, db_name)
            
            row = cursor.fetchone()
            if not row:
                return {"exists": False}
            
            return {
                "exists": True,
                "state": row[0],
                "user_access": row[1],
                "read_only": row[2],
                "recovery_model": row[3],
            }
            
        finally:
            conn.close()

    def get_table_list(self, db_name: str) -> list[str]:
        """
        Get list of user tables in a database.
        
        Parameters
        ----------
        db_name : str
            Database name.
        
        Returns
        -------
        list[str]
            List of table names.
        """
        conn = self._get_connection(db_name)
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            
            return [row[0] for row in cursor.fetchall()]
            
        finally:
            conn.close()

    def get_table_row_counts(self, db_name: str) -> dict[str, int]:
        """
        Get row counts for all tables in a database.
        
        Parameters
        ----------
        db_name : str
            Database name.
        
        Returns
        -------
        dict[str, int]
            Mapping of table names to row counts.
        """
        conn = self._get_connection(db_name)
        try:
            cursor = conn.cursor()
            
            # Use sys.dm_db_partition_stats for efficient row counts
            cursor.execute("""
                SELECT 
                    t.name AS table_name,
                    SUM(p.rows) AS row_count
                FROM sys.tables t
                INNER JOIN sys.dm_db_partition_stats p 
                    ON t.object_id = p.object_id
                WHERE p.index_id IN (0, 1)  -- Heap or clustered index
                GROUP BY t.name
                ORDER BY t.name
            """)
            
            return {row[0]: row[1] for row in cursor.fetchall()}
            
        finally:
            conn.close()
