#!/usr/bin/env python3
"""
upload_mapping_overrides.py — Upload table mapping overrides to S3.

Uploads per-tenant or per-table mapping configuration overrides to S3.
"""

import argparse
import json
import os
import sys

import boto3


def upload_overrides(bucket: str, prefix: str, tenant_id: str, overrides: dict) -> str:
    """
    Upload mapping overrides to S3.
    
    Parameters
    ----------
    bucket : str
        S3 bucket name.
    prefix : str
        S3 key prefix.
    tenant_id : str
        Tenant identifier.
    overrides : dict
        Mapping overrides.
    
    Returns
    -------
    str
        S3 URI of uploaded file.
    """
    s3 = boto3.client("s3")
    
    key = f"{prefix.rstrip('/')}/{tenant_id}/mapping_overrides.json"
    body = json.dumps(overrides, indent=2)
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    
    return f"s3://{bucket}/{key}"


def main():
    parser = argparse.ArgumentParser(
        description="Upload mapping overrides to S3"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        default="config/",
        help="S3 key prefix",
    )
    parser.add_argument(
        "--tenant-id",
        required=True,
        help="Tenant identifier",
    )
    parser.add_argument(
        "--overrides-file",
        required=True,
        help="Path to overrides JSON file",
    )
    args = parser.parse_args()
    
    try:
        with open(args.overrides_file, "r") as f:
            overrides = json.load(f)
        
        s3_uri = upload_overrides(args.bucket, args.prefix, args.tenant_id, overrides)
        print(f"Uploaded to: {s3_uri}")
    except Exception as e:
        print(f"Failed to upload overrides: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
