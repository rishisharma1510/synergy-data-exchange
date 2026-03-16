#!/usr/bin/env python3
"""
local_test.py — Local testing script for the extraction pipeline.

Runs a simplified extraction against local SQL Server for development.
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import AppConfig
from src.core.iceberg_reader import IcebergReader
from src.core.sql_writer import SqlServerWriter
from src.core.ingest import run_ingestion


def main():
    parser = argparse.ArgumentParser(
        description="Run local extraction test"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Iceberg table to extract (namespace.table)",
    )
    parser.add_argument(
        "--database",
        default="LocalTestDB",
        help="Target database name",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max rows to extract (for testing)",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file",
    )
    args = parser.parse_args()
    
    # Load configuration
    os.environ["SQL_DATABASE"] = args.database
    
    try:
        config = AppConfig.from_env(args.env_file)
        
        print(f"Testing extraction of {args.table}")
        print(f"Target database: {args.database}")
        print(f"Row limit: {args.limit}")
        
        result = run_ingestion(
            config=config,
            tables=[args.table],
            row_limit=args.limit,
        )
        
        print(f"\nExtraction complete:")
        print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"Extraction failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
