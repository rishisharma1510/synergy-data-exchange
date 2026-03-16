"""
progress_reporter.py — Activity Management API callbacks + S3 live progress.

Reports progress updates in two ways:
1. S3 — writes sde/runs/{run_id}/progress.json on every call.
   Your API can GET this object and return it to the UI on each poll.
2. HTTP callback — optional POST to an Activity Management API endpoint.
"""

import json
import logging
import queue
import threading
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ProgressReporter:
    """
    Reports progress to S3 (always) and optionally to an Activity Management API.

    S3 progress key: sde/runs/{run_id}/progress.json
    Always overwritten — your API reads this key on each UI poll.
    """

    def __init__(
        self,
        activity_id: str,
        callback_url: Optional[str] = None,
        run_id: Optional[str] = None,
        s3_bucket: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        activity_id : str
            Activity / run identifier.
        callback_url : str, optional
            Base URL for HTTP progress callback. Skipped when empty.
        run_id : str, optional
            Run ID used to build the S3 key. Falls back to activity_id.
        s3_bucket : str, optional
            Bucket to write progress.json into. S3 write skipped when not set.
        """
        self.callback_url = callback_url.rstrip("/") if callback_url else None
        self.activity_id = activity_id
        self.run_id = run_id or activity_id
        self.s3_bucket = s3_bucket
        self._s3_key = f"sde/runs/{self.run_id}/progress.json"
        self._s3_client = None
        self._session = None
        # Background writer thread for fire-and-forget S3 progress writes.
        # report() enqueues payloads here; the daemon thread does the actual
        # put_object so it never blocks the critical path.
        self._s3_queue: queue.Queue = queue.Queue()
        self._bg_thread = threading.Thread(
            target=self._s3_writer_loop,
            name="progress-s3-writer",
            daemon=True,
        )
        self._bg_thread.start()
    
    def _get_session(self):
        """Get or create an HTTP session."""
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
        return self._session

    def _get_s3(self):
        """Lazy boto3 S3 client."""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client("s3")
        return self._s3_client

    def _s3_writer_loop(self) -> None:
        """Background daemon loop — drains _s3_queue and calls put_object."""
        while True:
            payload = self._s3_queue.get()
            try:
                if payload is None:  # shutdown sentinel
                    return
                self._write_s3_progress_sync(payload)
            finally:
                self._s3_queue.task_done()

    def _write_s3_progress_sync(self, payload: dict) -> None:
        """
        Overwrite sde/runs/{run_id}/progress.json with current state.
        Non-fatal — never raises. Called from the background writer thread.
        """
        if not self.s3_bucket:
            return
        try:
            body = json.dumps(payload, default=str).encode("utf-8")
            self._get_s3().put_object(
                Bucket=self.s3_bucket,
                Key=self._s3_key,
                Body=body,
                ContentType="application/json",
            )
            logger.debug("Progress written to s3://%s/%s", self.s3_bucket, self._s3_key)
        except Exception as e:
            logger.warning("Failed to write S3 progress: %s", e)

    def _write_s3_progress(self, payload: dict) -> None:
        """Enqueue a progress payload for background S3 write (non-blocking)."""
        if not self.s3_bucket:
            return
        self._s3_queue.put(payload)

    def flush(self) -> None:
        """Block until all queued S3 progress writes have completed.

        Call this before the task exits to ensure the final COMPLETED / FAILED
        status is durably written to S3 before the container shuts down.
        """
        if self.s3_bucket:
            self._s3_queue.join()

    def report(
        self,
        progress_pct: int,
        status: str,
        *,
        details: Optional[dict] = None,
        manifest: Optional[dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Report progress.

        Always writes progress.json to S3 (if s3_bucket is set).
        Optionally POSTs to callback_url.

        Parameters
        ----------
        progress_pct : int
            Progress percentage (0-100).
        status : str
            Current status string (e.g. "LOADING", "COMPLETED", "FAILED").
        details : dict, optional
            Step-level detail (current table, rows, etc.).
        manifest : dict, optional
            Final summary payload (set on COMPLETED).
        error : str, optional
            Error message (set on FAILED).
        """
        payload = {
            "run_id": self.run_id,
            "activity_id": self.activity_id,
            "progress": progress_pct,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if details:
            payload["details"] = details
        if manifest:
            payload["manifest"] = manifest
        if error:
            payload["error"] = error

        logger.info("Reporting progress: %d%% - %s", progress_pct, status)

        # 1. Write to S3 — always, non-fatal
        self._write_s3_progress(payload)

        # 2. HTTP callback — optional
        if not self.callback_url:
            return
        url = f"{self.callback_url}/activities/{self.activity_id}/progress"
        try:
            session = self._get_session()
            response = session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            logger.debug("Progress callback sent OK.")
        except Exception as e:
            logger.warning("Failed to send progress callback: %s", e)
    
    def report_table_progress(
        self,
        table_name: str,
        rows_loaded: int,
        total_tables: int,
        tables_done: int,
    ) -> None:
        """
        Report fine-grained progress during table loading.
        
        Parameters
        ----------
        table_name : str
            Name of the table being loaded.
        rows_loaded : int
            Number of rows loaded for this table.
        total_tables : int
            Total number of tables to load.
        tables_done : int
            Number of tables completed.
        """
        # Calculate overall progress (20% to 70% range for loading phase)
        base_progress = 20
        loading_range = 50  # 20% to 70%
        
        progress_pct = base_progress + int(
            (tables_done / total_tables) * loading_range
        )
        
        self.report(
            progress_pct,
            "LOADING",
            details={
                "current_table": table_name,
                "rows_loaded": rows_loaded,
                "tables_done": tables_done,
                "total_tables": total_tables,
            },
        )
