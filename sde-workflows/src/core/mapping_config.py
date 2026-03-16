"""
mapping_config.py — Read/write mapping configuration files.

The mapping config is a JSON file keyed by ``{namespace}_{table}``,
supporting multiple source tables.  Each entry describes:
  - source / target identifiers
  - per-column mappings with strategy, confidence, and include flags
  - table-creation and load-mode settings

Workflow:
  Step 1 (generate):  auto-build the config from Iceberg + SQL Server schemas
  Step 2 (run):       read the (possibly user-edited) config and ingest
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa

from src.core.column_mapper import ColumnMapper, MappingResult, ColumnMatch
from src.core.sql_writer import _map_arrow_type

logger = logging.getLogger(__name__)

DEFAULT_MAPPING_FILE = "mapping_config.json"


# =====================================================================
# Data structures
# =====================================================================


def _table_key(namespace: str, table: str) -> str:
    """Build the dict key for a source table: ``namespace_table``."""
    return f"{namespace}_{table}"


def _build_column_entry(
    source: str,
    target: Optional[str],
    sql_type: Optional[str],
    source_type: str,
    strategy: str,
    confidence: float,
    include: bool,
) -> dict:
    return {
        "source": source,
        "target": target,
        "source_type": source_type,
        "target_type": sql_type,
        "strategy": strategy,
        "confidence": round(confidence, 2),
        "include": include,
    }


# =====================================================================
# Generate mapping config
# =====================================================================


def generate_mapping_for_table(
    *,
    namespace: str,
    table: str,
    target_schema: str,
    target_table: str,
    arrow_schema: pa.Schema,
    target_columns: Optional[list[dict]],
    mapper: ColumnMapper,
    naming_convention: str,
    load_mode: str,
) -> dict:
    """
    Produce a single-table mapping entry.

    Parameters
    ----------
    arrow_schema : pa.Schema
        Source schema from Iceberg.
    target_columns : list[dict] | None
        Existing SQL Server columns (None if the table doesn't exist).
    mapper : ColumnMapper
        The auto-mapping engine.
    """
    columns: list[dict] = []

    if target_columns:
        # === Target table exists — auto-map ===
        result: MappingResult = mapper.auto_map(arrow_schema, target_columns)

        for m in result.matched:
            columns.append(
                _build_column_entry(
                    source=m.source_name,
                    target=m.target_name,
                    sql_type=m.target_type,
                    source_type=m.source_type,
                    strategy=m.strategy,
                    confidence=m.confidence,
                    include=True,
                )
            )
        for src_name in result.unmatched_source:
            # Find the source type
            src_field = arrow_schema.field(src_name)
            columns.append(
                _build_column_entry(
                    source=src_name,
                    target=None,
                    sql_type=None,
                    source_type=str(src_field.type),
                    strategy="unmatched",
                    confidence=0.0,
                    include=False,
                )
            )
        create_table = False
    else:
        # === Target table doesn't exist — use naming convention ===
        target_names = mapper.transform_names(arrow_schema)
        for field, tgt_name in zip(arrow_schema, target_names):
            sql_type = _map_arrow_type(field.type)
            columns.append(
                _build_column_entry(
                    source=field.name,
                    target=tgt_name,
                    sql_type=sql_type,
                    source_type=str(field.type),
                    strategy="naming-convention",
                    confidence=1.0,
                    include=True,
                )
            )
        create_table = True

    return {
        "source": {
            "namespace": namespace,
            "table": table,
        },
        "target": {
            "schema": target_schema,
            "table": target_table,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "naming_convention": naming_convention,
        "create_table_if_missing": create_table,
        "load_mode": load_mode,
        "columns": columns,
    }


# =====================================================================
# Read / write helpers
# =====================================================================


def load_mapping_file(path: str = DEFAULT_MAPPING_FILE) -> dict:
    """Load the full mapping config from disk."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Mapping config not found: {p.resolve()}\n"
            "Run without --run first to generate it."
        )
    data = json.loads(p.read_text(encoding="utf-8"))
    logger.info("Loaded mapping config from %s (%d table(s)).", p, len(data))
    return data


def save_mapping_file(data: dict, path: str = DEFAULT_MAPPING_FILE) -> Path:
    """Save the mapping config to disk, merging with any existing entries."""
    p = Path(path)

    # Merge with existing file if present
    if p.exists() and p.stat().st_size > 0:
        try:
            existing = json.loads(p.read_text(encoding="utf-8"))
            existing.update(data)
            data = existing
        except json.JSONDecodeError:
            pass

    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Mapping config saved to %s (%d table(s)).", p.resolve(), len(data))
    return p


# =====================================================================
# Convert mapping config entry → MappingResult for sql_writer
# =====================================================================


def mapping_entry_to_result(entry: dict) -> MappingResult:
    """Convert a mapping config dict entry into a MappingResult."""
    matched = []
    unmatched_source = []
    unmatched_target = []

    for col in entry.get("columns", []):
        if col.get("include", True) and col.get("target"):
            matched.append(
                ColumnMatch(
                    source_name=col["source"],
                    target_name=col["target"],
                    strategy=col.get("strategy", "config"),
                    confidence=col.get("confidence", 1.0),
                    source_type=col.get("source_type", ""),
                    target_type=col.get("target_type", ""),
                )
            )
        else:
            unmatched_source.append(col["source"])

    return MappingResult(
        matched=matched,
        unmatched_source=unmatched_source,
        unmatched_target=unmatched_target,
    )


def print_mapping_summary(entry: dict, key: str) -> None:
    """Print a human-readable summary of a mapping entry."""
    src = entry["source"]
    tgt = entry["target"]
    cols = entry.get("columns", [])
    included = [c for c in cols if c.get("include", True) and c.get("target")]
    excluded = [c for c in cols if not c.get("include", True) or not c.get("target")]

    logger.info("=" * 70)
    logger.info("  Table: %s", key)
    logger.info("  Source : %s.%s", src['namespace'], src['table'])
    logger.info("  Target : %s.%s", tgt['schema'], tgt['table'])
    logger.info("  Mode   : %s", entry.get('load_mode', 'full'))
    logger.info("  Create : %s", entry.get('create_table_if_missing', False))
    logger.info("=" * 70)
    logger.info("  %-30s  ->  %-30s  %-18s  %5s  INCLUDE", "SOURCE", "TARGET", "STRATEGY", "CONF")
    logger.info("  %s", "-" * 100)

    for col in cols:
        src_name = col["source"]
        tgt_name = col.get("target") or "(none)"
        strategy = col.get("strategy", "")
        confidence = col.get("confidence", 0.0)
        include = col.get("include", True)
        marker = "Y" if include and col.get("target") else "N"
        logger.info(
            "  %-30s  ->  %-30s  %-18s  %4.0f%%  %s",
            src_name, tgt_name, strategy, confidence * 100, marker,
        )

    logger.info("  Included: %d  |  Excluded: %d", len(included), len(excluded))
