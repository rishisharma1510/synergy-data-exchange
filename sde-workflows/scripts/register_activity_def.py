#!/usr/bin/env python3
"""
register_activity_def.py — Register Activity Definition with Activity Management.

Registers the extraction activity type with the Activity Management API.
"""

import argparse
import json
import os
import sys

import requests


def register_activity(api_url: str, activity_type: str, schema: dict) -> dict:
    """
    Register an activity definition.
    
    Parameters
    ----------
    api_url : str
        Activity Management API base URL.
    activity_type : str
        Activity type name.
    schema : dict
        Activity input/output schema.
    
    Returns
    -------
    dict
        Registration response.
    """
    payload = {
        "activityType": activity_type,
        "schema": schema,
        "description": "Iceberg to SQL Server extraction pipeline",
    }
    
    response = requests.post(
        f"{api_url}/activities/register",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Register extraction activity with Activity Management"
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("ACTIVITY_API_URL", "https://activity-api.example.com"),
        help="Activity Management API URL",
    )
    parser.add_argument(
        "--activity-type",
        default="data-extraction",
        help="Activity type name",
    )
    args = parser.parse_args()
    
    schema = {
        "input": {
            "type": "object",
            "properties": {
                "tenant_id": {"type": "string"},
                "exposure_ids": {"type": "array", "items": {"type": "integer"}},
                "tables": {"type": "array", "items": {"type": "string"}},
                "artifact_type": {"type": "string", "enum": ["bak", "mdf", "both"]},
            },
            "required": ["tenant_id", "exposure_ids"],
        },
        "output": {
            "type": "object",
            "properties": {
                "manifest_s3_uri": {"type": "string"},
                "artifacts": {"type": "array"},
            },
        },
    }
    
    try:
        result = register_activity(args.api_url, args.activity_type, schema)
        print(f"Activity registered: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"Failed to register activity: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
