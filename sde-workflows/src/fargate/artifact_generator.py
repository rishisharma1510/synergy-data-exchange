"""
artifact_generator.py — SQL Server backup and detach operations.

Generates .bak (backup) and .mdf/.ldf (detached database files) artifacts.
"""

import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyodbc

from src.core.config import SqlServerConfig

logger = logging.getLogger(__name__)


def local_path_to_unc(local_path: str, server_host: str, share_path: str = "") -> str:
    """
    Convert a local Windows path to an accessible network path.
    
    If share_path is provided, it's used as the base UNC path.
    Otherwise, falls back to admin share (\\\\server\\c$\\...).
    
    Parameters
    ----------
    local_path : str
        Local path on the SQL Server, e.g. C:\\Data\\file.mdf
    server_host : str
        SQL Server host/IP
    share_path : str
        Optional UNC share path that maps to SQL Server's data directory.
        e.g., if SQL data is at C:\\SQLDATA and you have \\\\server\\sqlshare mapped there,
        set share_path = "\\\\server\\sqlshare" and local paths starting with C:\\SQLDATA
        will be translated.
        
        Or use format "C:\\SQLDATA=\\\\server\\sqlshare" for explicit mapping.
    
    Returns
    -------
    str
        Accessible path (UNC or original if local)
    """
    if not local_path:
        return local_path
    
    # If a share mapping is provided with explicit source=dest format
    if share_path and "=" in share_path:
        local_prefix, unc_prefix = share_path.split("=", 1)
        local_prefix = local_prefix.strip()
        unc_prefix = unc_prefix.strip()
        if local_path.lower().startswith(local_prefix.lower()):
            remainder = local_path[len(local_prefix):]
            return f"{unc_prefix}{remainder}"
    
    # If a share path is provided, assume it maps to the same directory
    if share_path:
        # Extract just the filename and append to share
        file_name = os.path.basename(local_path)
        return os.path.join(share_path, file_name)
    
    # Fall back to admin share: C:\path\to\file -> \\server\c$\path\to\file
    if len(local_path) >= 2 and local_path[1] == ":":
        drive = local_path[0].lower()
        remainder = local_path[2:]  # \path\to\file
        return f"\\\\{server_host}\\{drive}${remainder}"
    
    return local_path


@dataclass
class ArtifactFile:
    """Represents a generated artifact file."""
    file_path: str
    file_type: str  # "bak" | "mdf" | "ldf"
    size_bytes: int
    checksum_sha256: str


class ArtifactGenerator:
    """Generates SQL Server backup and database file artifacts."""
    
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
    
    def generate_bak(self, db_name: str, output_path: str) -> ArtifactFile:
        """
        Generate a .bak backup file.
        
        Parameters
        ----------
        db_name : str
            Source database name.
        output_path : str
            Directory to write the backup file.
        
        Returns
        -------
        ArtifactFile
            Metadata about the generated backup.
        
        Notes
        -----
        SQL Server Express does not support WITH COMPRESSION, so backups
        will be uncompressed.
        """
        import time as _time
        Path(output_path).mkdir(parents=True, exist_ok=True)
        bak_file = os.path.join(output_path, f"{db_name}.bak")

        logger.info("[generate_bak] db=%s target=%s", db_name, bak_file)
        t0 = _time.perf_counter()

        # Step 1: Force a CHECKPOINT on the target database so all dirty pages are
        # flushed to disk before backup starts.  This gives the backup engine a
        # clean, consistent starting point and avoids races between the checkpoint
        # thread and the backup writer that can trigger Error 3041 on Express.
        conn_chk = self._get_connection(db_name)
        try:
            conn_chk.cursor().execute("CHECKPOINT")
            logger.info("[generate_bak] db=%s CHECKPOINT complete", db_name)
        finally:
            conn_chk.close()

        # Step 2: Run backup via sqlcmd subprocess.
        #
        # pyodbc does NOT raise a Python exception for SQL Server Error 3041
        # ("BACKUP failed to complete") because that error is delivered as a
        # deferred TDS message AFTER cursor.execute() has already returned.
        # sqlcmd with the -b flag exits with return code 1 on any error
        # severity >= 16, giving us a reliable failure signal.
        #
        # Trace flag 3222 disables the read-ahead lock that can stall the
        # backup media writer on overlay2 / Fargate ephemeral storage and is
        # a known workaround for sporadic Error 3041 on SQL Server Linux.
        import subprocess as _subprocess
        sa_password = (
            os.environ.get('SA_PASSWORD')
            or self.cfg.password  # fall back to configured credential
        )
        if not sa_password:
            raise RuntimeError(
                "[generate_bak] Cannot determine SA password for sqlcmd backup. "
                "Set SA_PASSWORD env var or ensure cfg.password is populated."
            )

        sqlcmd_path = '/opt/mssql-tools18/bin/sqlcmd'
        if not os.path.exists(sqlcmd_path):
            sqlcmd_path = '/opt/mssql-tools/bin/sqlcmd'  # older image fallback

        backup_sql = (
            f"DBCC TRACEON(3222, -1); "
            f"BACKUP DATABASE [{db_name}] TO DISK = N'{bak_file}' "
            f"WITH COPY_ONLY, INIT, NAME = N'{db_name} - Full Backup'; "
            f"DBCC TRACEOFF(3222, -1);"
        )
        logger.info("[generate_bak] db=%s running BACKUP DATABASE via sqlcmd...", db_name)
        proc = _subprocess.run(
            [sqlcmd_path, '-S', 'localhost', '-U', 'sa', '-P', sa_password,
             '-C',   # trust server certificate
             '-b',   # exit code 1 on any SQL error severity >= 16
             '-Q', backup_sql],
            capture_output=True, text=True, timeout=600,
        )
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()
        if stdout:
            logger.info("[generate_bak] db=%s sqlcmd output: %s", db_name, stdout)
        if stderr:
            logger.warning("[generate_bak] db=%s sqlcmd stderr: %s", db_name, stderr)
        if proc.returncode != 0:
            raise RuntimeError(
                f"[generate_bak] BACKUP DATABASE failed (sqlcmd exit code {proc.returncode}). "
                f"Output: {(stdout + ' ' + stderr).strip()}"
            )
        logger.info("[generate_bak] db=%s BACKUP DATABASE complete in %.2fs", db_name, _time.perf_counter() - t0)

        # Step 3: Size validation — a real backup for even a tiny database is
        # always several hundred KB.  Zero-byte or missing files indicate that
        # sqlcmd exit code was misleading (shouldn't happen with -b, but belt+braces).
        MIN_BAK_BYTES = 64 * 1024  # 64 KB minimum
        if not os.path.exists(bak_file):
            raise RuntimeError(
                f"[generate_bak] sqlcmd reported success but '{bak_file}' does not exist."
            )
        size = os.path.getsize(bak_file)
        if size < MIN_BAK_BYTES:
            raise RuntimeError(
                f"[generate_bak] BAK file '{bak_file}' is only {size} bytes — backup likely failed."
            )
        logger.info("[generate_bak] db=%s computing checksum...", db_name)
        t_cs = _time.perf_counter()
        checksum = self.compute_checksum(bak_file)
        logger.info("[generate_bak] db=%s done — size=%.2f MB checksum=%.2fs sha256=%.12s...",
            db_name, size / 1_048_576, _time.perf_counter() - t_cs, checksum)

        return ArtifactFile(
            file_path=bak_file,
            file_type="bak",
            size_bytes=size,
            checksum_sha256=checksum,
        )
    
    def generate_mdf(self, db_name: str, output_dir: str) -> list[ArtifactFile]:
        """
        Generate .mdf and .ldf files by detaching the database.
        
        Parameters
        ----------
        db_name : str
            Source database name.
        output_dir : str
            Directory to copy the files to.
        
        Returns
        -------
        list[ArtifactFile]
            List of generated files (.mdf and .ldf).
        
        Notes
        -----
        After detaching, the database will no longer be accessible.
        For remote SQL Servers, uses UNC paths via admin shares.
        """
        import time as _time
        import shutil
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        host = self.cfg.host
        is_local = host.lower() in ("localhost", "127.0.0.1", ".", "(local)")
        logger.info("[generate_mdf] db=%s host=%s is_local=%s output_dir=%s",
            db_name, host, is_local, output_dir)

        conn = self._get_connection("master")
        artifacts = []

        try:
            cursor = conn.cursor()

            # Resolve physical file paths BEFORE detaching
            t0 = _time.perf_counter()
            cursor.execute(f"""
                SELECT physical_name, type_desc
                FROM sys.master_files
                WHERE database_id = DB_ID('{db_name}')
            """)
            files = [(row.physical_name, row.type_desc) for row in cursor.fetchall()]
            logger.info("[generate_mdf] db=%s found %d file(s) in sys.master_files (%.2fs): %s",
                db_name, len(files), _time.perf_counter() - t0, [f[0] for f in files])

            if not files:
                raise ValueError(f"No files found in sys.master_files for database {db_name}")

            # Set SINGLE_USER to kick other connections
            logger.info("[generate_mdf] db=%s setting SINGLE_USER with ROLLBACK IMMEDIATE...", db_name)
            t0 = _time.perf_counter()
            cursor.execute(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            try:
                cursor.fetchall()
            except Exception:
                pass
            logger.info("[generate_mdf] db=%s SINGLE_USER set in %.2fs", db_name, _time.perf_counter() - t0)

            # Detach
            logger.info("[generate_mdf] db=%s executing sp_detach_db...", db_name)
            t0 = _time.perf_counter()
            cursor.execute(f"EXEC sp_detach_db N'{db_name}'")
            try:
                cursor.fetchall()
            except Exception:
                pass
            logger.info("[generate_mdf] db=%s detached in %.2fs", db_name, _time.perf_counter() - t0)

            # Copy each file to output_dir
            share_path = self.cfg.data_share_path
            for physical_path, type_desc in files:
                file_type = "mdf" if type_desc == "ROWS" else "ldf"
                if is_local:
                    source_path = physical_path
                else:
                    source_path = local_path_to_unc(physical_path, host, share_path)
                    logger.info("[generate_mdf] db=%s UNC path: %s -> %s", db_name, physical_path, source_path)

                if not os.path.exists(source_path):
                    logger.error("[generate_mdf] db=%s file NOT FOUND at path: %s", db_name, source_path)
                    continue

                file_name = os.path.basename(physical_path)
                dest_path = os.path.join(output_dir, file_name)
                src_size = os.path.getsize(source_path)
                logger.info("[generate_mdf] db=%s copying %s -> %s (%.2f MB)...",
                    db_name, source_path, dest_path, src_size / 1_048_576)
                t0 = _time.perf_counter()
                shutil.copy2(source_path, dest_path)
                copy_elapsed = _time.perf_counter() - t0

                logger.info("[generate_mdf] db=%s checksum for %s...", db_name, file_name)
                t0 = _time.perf_counter()
                checksum = self.compute_checksum(dest_path)
                size = os.path.getsize(dest_path)
                logger.info(
                    "[generate_mdf] db=%s %s done — type=%s size=%.2f MB copy=%.2fs checksum=%.2fs sha256=%.12s...",
                    db_name, file_name, file_type, size / 1_048_576,
                    copy_elapsed, _time.perf_counter() - t0, checksum,
                )

                artifacts.append(ArtifactFile(
                    file_path=dest_path,
                    file_type=file_type,
                    size_bytes=size,
                    checksum_sha256=checksum,
                ))

        finally:
            conn.close()

        logger.info("[generate_mdf] db=%s returning %d artifact(s)", db_name, len(artifacts))
        return artifacts
    
    def generate(
        self,
        db_name: str,
        artifact_type: str,
        output_dir: str,
    ) -> list[ArtifactFile]:
        """
        Generate artifacts based on the requested type.
        
        Parameters
        ----------
        db_name : str
            Source database name.
        artifact_type : str
            "bak", "mdf", or "both".
        output_dir : str
            Directory to write artifacts.
        
        Returns
        -------
        list[ArtifactFile]
            List of generated artifacts.
        """
        artifacts = []
        
        if artifact_type in ("bak", "both"):
            bak = self.generate_bak(db_name, output_dir)
            artifacts.append(bak)
        
        if artifact_type in ("mdf", "both"):
            mdf_files = self.generate_mdf(db_name, output_dir)
            artifacts.extend(mdf_files)
        
        logger.info(
            "Generated %d artifacts (total: %d bytes)",
            len(artifacts),
            sum(a.size_bytes for a in artifacts),
        )
        
        return artifacts
    
    @staticmethod
    def compute_checksum(file_path: str, algorithm: str = "sha256") -> str:
        """
        Compute checksum of a file without loading into memory.
        
        Parameters
        ----------
        file_path : str
            Path to the file.
        algorithm : str
            Hash algorithm (default: sha256).
        
        Returns
        -------
        str
            Hex-encoded checksum.
        """
        hash_obj = hashlib.new(algorithm)
        
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_obj.update(chunk)
        
        return hash_obj.hexdigest()
