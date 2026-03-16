"""
artifact_restorer.py — Restore SQL Server database from BAK file or attach from MDF.

Reverse of artifact_generator.py — used for ingestion flow.
Handles RESTORE DATABASE and CREATE DATABASE FOR ATTACH operations.
"""

import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

import pyodbc

from src.core.config import SqlServerConfig

logger = logging.getLogger(__name__)


class ArtifactRestorer:
    """Restore SQL Server databases from backup artifacts."""
    
    def __init__(
        self, 
        config: SqlServerConfig, 
        data_dir: str = "/data",
    ) -> None:
        """
        Initialize the artifact restorer.
        
        Parameters
        ----------
        config : SqlServerConfig
            SQL Server connection configuration.
        data_dir : str
            Base directory for data files (default: /data).
        """
        self.config = config
        self.data_dir = Path(data_dir)
        self.backup_dir = self.data_dir / "backup"
        self.mdf_dir = self.data_dir / "mdf"
        self._connection: Optional[pyodbc.Connection] = None
        
        # Ensure directories exist
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.mdf_dir.mkdir(parents=True, exist_ok=True)
    
    def connect(self) -> None:
        """Connect to master database for restore operations."""
        conn_str = (
            f"DRIVER={{{self.config.driver}}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE=master;"
            f"UID={self.config.username};"
            f"PWD={self.config.password};"
            f"Connection Timeout={self.config.connection_timeout};"
        )
        
        if self.config.trust_cert:
            conn_str += "TrustServerCertificate=yes;"
        
        logger.info("Connecting to SQL Server master database for restore operations")
        self._connection = pyodbc.connect(conn_str, autocommit=True)
        logger.info("Connected to master database")
    
    def verify_checksum(self, file_path: Path, expected_checksum: str) -> bool:
        """
        Verify file integrity using SHA256.
        
        Parameters
        ----------
        file_path : Path
            Path to the file to verify.
        expected_checksum : str
            Expected SHA256 hex digest.
        
        Returns
        -------
        bool
            True if checksum matches.
        """
        logger.info("Verifying checksum for %s", file_path)
        sha256 = hashlib.sha256()
        
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        
        actual = sha256.hexdigest()
        matches = actual.lower() == expected_checksum.lower()
        
        if matches:
            logger.info("Checksum verified: %s", expected_checksum[:16] + "...")
        else:
            logger.error(
                "Checksum mismatch! Expected: %s, Got: %s",
                expected_checksum[:16] + "...",
                actual[:16] + "...",
            )
        
        return matches
    
    def get_backup_file_list(self, bak_path: Path) -> list[dict]:
        """
        Get the list of logical files in a backup file.
        
        Parameters
        ----------
        bak_path : Path
            Path to the .bak file.
        
        Returns
        -------
        list[dict]
            List of file info dicts with logical_name, physical_name, type.
        """
        self._ensure_connected()
        cursor = self._connection.cursor()
        
        cursor.execute(f"RESTORE FILELISTONLY FROM DISK = N'{bak_path}'")
        
        files = []
        for row in cursor.fetchall():
            files.append({
                "logical_name": row[0],
                "physical_name": row[1],
                "type": row[2],  # 'D' for data, 'L' for log
                "size": row[4] if len(row) > 4 else 0,
            })
        
        return files
    
    def restore_from_bak(
        self, 
        bak_file: str, 
        database_name: str,
        expected_checksum: Optional[str] = None,
        with_recovery: bool = True,
    ) -> str:
        """
        Restore database from .bak file.
        
        Parameters
        ----------
        bak_file : str
            Path to .bak file (relative to backup_dir or absolute).
        database_name : str
            Name for the restored database.
        expected_checksum : str, optional
            SHA256 checksum to verify before restore.
        with_recovery : bool
            Whether to recover the database (default: True).
        
        Returns
        -------
        str
            Restored database name.
        
        Raises
        ------
        FileNotFoundError
            If BAK file doesn't exist.
        ValueError
            If checksum verification fails.
        """
        bak_path = Path(bak_file)
        if not bak_path.is_absolute():
            bak_path = self.backup_dir / bak_file
        
        if not bak_path.exists():
            raise FileNotFoundError(f"BAK file not found: {bak_path}")
        
        # Verify checksum if provided
        if expected_checksum:
            if not self.verify_checksum(bak_path, expected_checksum):
                raise ValueError(f"Checksum verification failed for {bak_path}")
        
        self._ensure_connected()
        cursor = self._connection.cursor()
        
        # Sanitize database name
        db_name = self._sanitize_db_name(database_name)
        
        logger.info("Restoring database '%s' from %s", db_name, bak_path)
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM sys.databases WHERE name = ?",
            db_name,
        )
        if cursor.fetchone():
            logger.warning("Database %s exists — dropping first", db_name)
            self._drop_database(db_name)
        
        # Get logical file names from backup
        files = self.get_backup_file_list(bak_path)
        
        if not files:
            raise ValueError(f"No files found in backup: {bak_path}")
        
        # Build RESTORE command with MOVE clauses
        move_clauses = []
        for file_info in files:
            logical_name = file_info["logical_name"]
            file_type = file_info["type"]
            
            if file_type == "D":
                target_path = self.mdf_dir / f"{db_name}.mdf"
            else:
                target_path = self.mdf_dir / f"{db_name}_log.ldf"
            
            move_clauses.append(f"MOVE N'{logical_name}' TO N'{target_path}'")
        
        recovery = "RECOVERY" if with_recovery else "NORECOVERY"
        
        restore_sql = f"""
            RESTORE DATABASE [{db_name}]
            FROM DISK = N'{bak_path}'
            WITH 
                {', '.join(move_clauses)},
                REPLACE,
                {recovery}
        """
        
        logger.debug("Executing: %s", restore_sql.strip())
        cursor.execute(restore_sql)
        
        logger.info("Database '%s' restored successfully", db_name)
        return db_name
    
    def attach_from_mdf(
        self, 
        mdf_file: str, 
        database_name: str,
        ldf_file: Optional[str] = None,
    ) -> str:
        """
        Attach database from .mdf file.
        
        Parameters
        ----------
        mdf_file : str
            Path to .mdf file (relative to mdf_dir or absolute).
        database_name : str
            Name for the attached database.
        ldf_file : str, optional
            Path to .ldf file. If not provided, log will be rebuilt.
        
        Returns
        -------
        str
            Attached database name.
        
        Raises
        ------
        FileNotFoundError
            If MDF file doesn't exist.
        """
        mdf_path = Path(mdf_file)
        if not mdf_path.is_absolute():
            mdf_path = self.mdf_dir / mdf_file
        
        if not mdf_path.exists():
            raise FileNotFoundError(f"MDF file not found: {mdf_path}")
        
        self._ensure_connected()
        cursor = self._connection.cursor()
        
        # Sanitize database name
        db_name = self._sanitize_db_name(database_name)
        
        logger.info("Attaching database '%s' from %s", db_name, mdf_path)
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM sys.databases WHERE name = ?",
            db_name,
        )
        if cursor.fetchone():
            logger.warning("Database %s exists — dropping first", db_name)
            self._drop_database(db_name)
        
        if ldf_file:
            ldf_path = Path(ldf_file)
            if not ldf_path.is_absolute():
                ldf_path = self.mdf_dir / ldf_file
            
            attach_sql = f"""
                CREATE DATABASE [{db_name}]
                ON (FILENAME = N'{mdf_path}'),
                   (FILENAME = N'{ldf_path}')
                FOR ATTACH
            """
        else:
            # Attach with log rebuild
            attach_sql = f"""
                CREATE DATABASE [{db_name}]
                ON (FILENAME = N'{mdf_path}')
                FOR ATTACH_REBUILD_LOG
            """
        
        logger.debug("Executing: %s", attach_sql.strip())
        cursor.execute(attach_sql)
        
        logger.info("Database '%s' attached successfully", db_name)
        return db_name
    
    def validate_restore(self, database_name: str) -> dict:
        """
        Validate a restored database is online and accessible.
        
        Parameters
        ----------
        database_name : str
            Name of the database to validate.
        
        Returns
        -------
        dict
            Validation results with status, table_count, etc.
        """
        self._ensure_connected()
        cursor = self._connection.cursor()
        
        # Check database state
        cursor.execute("""
            SELECT state_desc, user_access_desc, is_read_only
            FROM sys.databases
            WHERE name = ?
        """, database_name)
        
        row = cursor.fetchone()
        if not row:
            return {
                "valid": False,
                "error": f"Database '{database_name}' not found",
            }
        
        state, access, readonly = row
        
        if state != "ONLINE":
            return {
                "valid": False,
                "error": f"Database is {state}, expected ONLINE",
            }
        
        # Count tables
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM [{database_name}].INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        table_count = cursor.fetchone()[0]
        
        return {
            "valid": True,
            "state": state,
            "access": access,
            "read_only": readonly,
            "table_count": table_count,
        }
    
    def cleanup_database(self, database_name: str) -> None:
        """
        Drop a database and clean up associated files.
        
        Parameters
        ----------
        database_name : str
            Name of the database to drop.
        """
        try:
            self._drop_database(database_name)
        except Exception as e:
            logger.warning("Failed to drop database %s: %s", database_name, e)
        
        # Clean up MDF/LDF files
        for ext in ["mdf", "ldf"]:
            file_path = self.mdf_dir / f"{database_name}.{ext}"
            if file_path.exists():
                try:
                    os.remove(file_path)
                    logger.info("Removed file: %s", file_path)
                except Exception as e:
                    logger.warning("Failed to remove %s: %s", file_path, e)
        
        # Also check for _log.ldf
        log_path = self.mdf_dir / f"{database_name}_log.ldf"
        if log_path.exists():
            try:
                os.remove(log_path)
                logger.info("Removed file: %s", log_path)
            except Exception as e:
                logger.warning("Failed to remove %s: %s", log_path, e)
    
    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    
    def _ensure_connected(self) -> None:
        """Ensure we have an active connection."""
        if self._connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
    
    def _sanitize_db_name(self, name: str) -> str:
        """
        Sanitize database name to prevent SQL injection.
        
        Parameters
        ----------
        name : str
            Raw database name.
        
        Returns
        -------
        str
            Sanitized database name (alphanumeric + underscore only).
        """
        import re
        # Keep only alphanumeric and underscore
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        # Ensure it starts with a letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = "_" + sanitized
        # Limit length (SQL Server max is 128)
        sanitized = sanitized[:128]
        return sanitized
    
    def _drop_database(self, db_name: str) -> None:
        """Drop a database with force disconnect."""
        cursor = self._connection.cursor()
        
        cursor.execute(f"""
            IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{db_name}')
            BEGIN
                ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{db_name}];
            END
        """)
        
        logger.info("Dropped database: %s", db_name)
