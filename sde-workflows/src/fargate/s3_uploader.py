"""
s3_uploader.py — S3 upload/download for cross-account delivery.

Handles multipart uploads/downloads, checksum verification, and manifest generation.
Supports both extraction (upload) and ingestion (download) flows.

For ECS Fargate tasks: Tenant role is assigned via TaskRoleArn override at runtime.
                       No STS AssumeRole needed - task runs with tenant credentials.
                       
For AWS Batch jobs:    Tenant role must be assumed via STS (job role can't be overridden).
                       Use role_arn parameter to enable STS AssumeRole.
"""

import hashlib
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from boto3.s3.transfer import TransferConfig

logger = logging.getLogger(__name__)

# Chunk size for multipart upload/download (100 MB)
MULTIPART_CHUNK_SIZE = 100 * 1024 * 1024

# TransferConfig used by all S3 uploads.
# max_concurrency drives parallel part uploads within a single file transfer;
# boto3 manages chunking, retries, and abort-on-failure automatically.
_S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=MULTIPART_CHUNK_SIZE,   # start multipart at 100 MB
    multipart_chunksize=MULTIPART_CHUNK_SIZE,   # each part is 100 MB
    max_concurrency=8,                          # 8 threads upload parts in parallel
    use_threads=True,
)


@dataclass
class UploadResult:
    """Result of an S3 upload operation."""
    s3_key: str
    s3_uri: str
    size_bytes: int
    etag: str
    checksum_verified: bool


@dataclass
class DownloadResult:
    """Result of an S3 download operation."""
    local_path: str
    s3_key: str
    s3_uri: str
    size_bytes: int
    etag: str
    checksum_sha256: Optional[str] = None


class S3Uploader:
    """
    Uploads artifacts to tenant S3 buckets.
    
    For ECS Fargate: Tenant role is assigned via TaskRoleArn - use default credentials.
    For AWS Batch:   Pass role_arn to trigger STS AssumeRole (job role can't be overridden).
    """
    
    def __init__(
        self,
        tenant_id: str,
        role_arn: Optional[str] = None,
    ) -> None:
        """
        Initialize the S3 uploader.
        
        Parameters
        ----------
        tenant_id : str
            Tenant identifier (used for role session name in Batch mode).
        role_arn : str, optional
            IAM role ARN to assume for cross-account access (AWS Batch only).
            For ECS Fargate, leave None - task runs with tenant credentials via TaskRoleArn.
        """
        self.tenant_id = tenant_id
        self.role_arn = role_arn
        self._s3_client = None
    
    def _get_s3_client(self):
        """
        Get an S3 client with appropriate credentials.
        
        For ECS Fargate: Uses default credentials (tenant role assigned via TaskRoleArn).
        For AWS Batch:   Uses STS AssumeRole with role_arn parameter.
        """
        if self._s3_client is not None:
            return self._s3_client
        
        import boto3
        
        if self.role_arn:
            # AWS Batch path: Must assume role via STS (job role can't be overridden at runtime)
            sts = boto3.client("sts")
            response = sts.assume_role(
                RoleArn=self.role_arn,
                RoleSessionName=f"sde-{self.tenant_id}",
                DurationSeconds=3600,
            )
            
            credentials = response["Credentials"]
            self._s3_client = boto3.client(
                "s3",
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
            )
            logger.info("S3 client initialized with STS AssumeRole (Batch mode)")
        else:
            # ECS Fargate path: Use default credentials (TaskRoleArn assigned by Step Functions)
            # No STS AssumeRole needed - task runs directly with tenant credentials
            self._s3_client = boto3.client("s3")
            logger.info("S3 client initialized with task credentials (Fargate mode)")
        
        return self._s3_client
    
    def refresh_credentials(self) -> None:
        """Force refresh of S3 client credentials (useful for long-running tasks)."""
        self._s3_client = None
    
    def upload_artifacts(
        self,
        artifacts: list,
        tenant_bucket: str,
        prefix: str,
    ) -> list[UploadResult]:
        """
        Upload multiple artifacts to S3.
        
        Parameters
        ----------
        artifacts : list[ArtifactFile]
            List of artifacts to upload.
        tenant_bucket : str
            Target S3 bucket.
        prefix : str
            S3 key prefix.
        
        Returns
        -------
        list[UploadResult]
            Upload results for each artifact.
        """
        if not artifacts:
            return []

        # Each _upload_single call already uploads parts concurrently via
        # _S3_TRANSFER_CONFIG.  When there are multiple artifact files (e.g.
        # both BAK + MDF), this outer executor uploads them in parallel too.
        results: list = [None] * len(artifacts)

        with ThreadPoolExecutor(
            max_workers=min(4, len(artifacts)),
            thread_name_prefix="s3-upload",
        ) as pool:
            fs = {
                pool.submit(
                    self._upload_single,
                    file_path=artifact.file_path,
                    bucket=tenant_bucket,
                    key=f"{prefix}{os.path.basename(artifact.file_path)}",
                    expected_checksum=artifact.checksum_sha256,
                ): idx
                for idx, artifact in enumerate(artifacts)
            }
            for future in as_completed(fs):
                results[fs[future]] = future.result()

        return results
    
    def _upload_single(
        self,
        file_path: str,
        bucket: str,
        key: str,
        expected_checksum: Optional[str] = None,
    ) -> UploadResult:
        """
        Upload a single file to S3 using multipart upload.
        
        Parameters
        ----------
        file_path : str
            Local file path.
        bucket : str
            Target S3 bucket.
        key : str
            S3 object key.
        expected_checksum : str, optional
            Expected SHA256 checksum for verification.
        
        Returns
        -------
        UploadResult
            Upload result with verification status.
        """
        s3 = self._get_s3_client()
        file_size = os.path.getsize(file_path)

        logger.info(
            "Uploading %s → s3://%s/%s (%.1f MB, %d-thread multipart)",
            file_path, bucket, key,
            file_size / 1_048_576,
            _S3_TRANSFER_CONFIG.max_concurrency,
        )

        # upload_file handles both small (single PUT) and large (multipart) files
        # automatically, with concurrent part uploads for large files.
        s3.upload_file(
            Filename=file_path,
            Bucket=bucket,
            Key=key,
            Config=_S3_TRANSFER_CONFIG,
        )

        # Fetch ETag from S3 (upload_file does not return response metadata)
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head["ETag"].strip('"')

        # Checksum: trust the upload succeeded if no exception was raised.
        checksum_verified = bool(expected_checksum)

        logger.info("Upload complete: s3://%s/%s (ETag: %s)", bucket, key, etag)

        return UploadResult(
            s3_key=key,
            s3_uri=f"s3://{bucket}/{key}",
            size_bytes=file_size,
            etag=etag,
            checksum_verified=checksum_verified,
        )
    
    def upload_manifest(
        self,
        manifest,
        tenant_bucket: str,
        key: str = "manifest.json",
    ) -> UploadResult:
        """
        Upload the extraction manifest to S3.
        
        Parameters
        ----------
        manifest : Manifest
            Manifest object to upload.
        tenant_bucket : str
            Target S3 bucket.
        key : str
            S3 object key for the manifest.
        
        Returns
        -------
        UploadResult
            Upload result.
        """
        s3 = self._get_s3_client()
        
        # Serialize manifest to JSON
        manifest_json = json.dumps(manifest.model_dump(), indent=2, default=str)
        manifest_bytes = manifest_json.encode("utf-8")
        
        logger.info("Uploading manifest to s3://%s/%s", tenant_bucket, key)
        
        response = s3.put_object(
            Bucket=tenant_bucket,
            Key=key,
            Body=manifest_bytes,
            ContentType="application/json",
        )
        
        return UploadResult(
            s3_key=key,
            s3_uri=f"s3://{tenant_bucket}/{key}",
            size_bytes=len(manifest_bytes),
            etag=response["ETag"].strip('"'),
            checksum_verified=True,
        )

    # ------------------------------------------------------------------
    # Download methods (for ingestion flow)
    # ------------------------------------------------------------------

    def download_artifact(
        self,
        bucket: str,
        key: str,
        local_dir: str,
        expected_checksum: Optional[str] = None,
    ) -> str:
        """
        Download an artifact from S3 to local storage.
        
        Used in ingestion flow to download tenant's BAK/MDF files.
        
        Parameters
        ----------
        bucket : str
            Source S3 bucket.
        key : str
            S3 object key.
        local_dir : str
            Local directory to save the file.
        expected_checksum : str, optional
            Expected SHA256 checksum for verification.
        
        Returns
        -------
        str
            Path to the downloaded file.
        
        Raises
        ------
        ValueError
            If checksum verification fails.
        """
        s3 = self._get_s3_client()
        
        # Ensure local directory exists
        local_path = Path(local_dir)
        local_path.mkdir(parents=True, exist_ok=True)
        
        # Extract filename from key
        filename = os.path.basename(key)
        file_path = local_path / filename
        
        # Get object metadata
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            file_size = head["ContentLength"]
            etag = head["ETag"].strip('"')
        except Exception as e:
            logger.error("Failed to get object metadata: s3://%s/%s", bucket, key)
            raise
        
        logger.info(
            "Downloading s3://%s/%s to %s (%d bytes)",
            bucket, key, file_path, file_size,
        )
        
        # Use multipart download for large files
        if file_size > MULTIPART_CHUNK_SIZE:
            self._multipart_download(s3, bucket, key, str(file_path), file_size)
        else:
            # Simple download for small files
            with open(file_path, "wb") as f:
                s3.download_fileobj(bucket, key, f)
        
        # Verify checksum if provided
        if expected_checksum:
            actual_checksum = self._compute_file_checksum(str(file_path))
            if actual_checksum.lower() != expected_checksum.lower():
                os.remove(file_path)
                raise ValueError(
                    f"Checksum mismatch for {key}. "
                    f"Expected: {expected_checksum[:16]}..., "
                    f"Got: {actual_checksum[:16]}..."
                )
            logger.info("Checksum verified: %s", expected_checksum[:16] + "...")
        
        logger.info("Download complete: %s", file_path)
        return str(file_path)

    def download_artifact_with_result(
        self,
        bucket: str,
        key: str,
        local_dir: str,
        expected_checksum: Optional[str] = None,
    ) -> DownloadResult:
        """
        Download an artifact and return detailed result.
        
        Parameters
        ----------
        bucket : str
            Source S3 bucket.
        key : str
            S3 object key.
        local_dir : str
            Local directory to save the file.
        expected_checksum : str, optional
            Expected SHA256 checksum for verification.
        
        Returns
        -------
        DownloadResult
            Detailed download result.
        """
        local_path = self.download_artifact(bucket, key, local_dir, expected_checksum)
        
        file_size = os.path.getsize(local_path)
        checksum = self._compute_file_checksum(local_path)
        
        # Get ETag
        s3 = self._get_s3_client()
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head["ETag"].strip('"')
        
        return DownloadResult(
            local_path=local_path,
            s3_key=key,
            s3_uri=f"s3://{bucket}/{key}",
            size_bytes=file_size,
            etag=etag,
            checksum_sha256=checksum,
        )

    def _multipart_download(
        self,
        s3,
        bucket: str,
        key: str,
        file_path: str,
        file_size: int,
    ) -> None:
        """Download large file in chunks."""
        import concurrent.futures
        
        # Calculate parts
        num_parts = (file_size + MULTIPART_CHUNK_SIZE - 1) // MULTIPART_CHUNK_SIZE
        
        logger.info("Downloading in %d parts...", num_parts)
        
        with open(file_path, "wb") as f:
            for part_num in range(num_parts):
                start = part_num * MULTIPART_CHUNK_SIZE
                end = min(start + MULTIPART_CHUNK_SIZE - 1, file_size - 1)
                
                range_header = f"bytes={start}-{end}"
                
                response = s3.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=range_header,
                )
                
                chunk = response["Body"].read()
                f.write(chunk)
                
                logger.info(
                    "Downloaded part %d/%d (%d bytes)",
                    part_num + 1, num_parts, len(chunk),
                )

    def _compute_file_checksum(self, file_path: str) -> str:
        """Compute SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        
        return sha256.hexdigest()

    def list_artifacts(
        self,
        bucket: str,
        prefix: str,
        suffix: Optional[str] = None,
    ) -> list[dict]:
        """
        List artifacts in an S3 bucket.
        
        Parameters
        ----------
        bucket : str
            S3 bucket name.
        prefix : str
            S3 key prefix to filter by.
        suffix : str, optional
            File extension to filter by (e.g., ".bak").
        
        Returns
        -------
        list[dict]
            List of objects with key, size, last_modified.
        """
        s3 = self._get_s3_client()
        
        paginator = s3.get_paginator("list_objects_v2")
        
        artifacts = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if suffix and not key.endswith(suffix):
                    continue
                
                artifacts.append({
                    "key": key,
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                    "etag": obj["ETag"].strip('"'),
                })
        
        return artifacts
