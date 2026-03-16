"""
column_mapper.py — Automatic column mapping between Iceberg and SQL Server.

Uses a multi-layer matching strategy (no manual mapping needed):
  1. Exact match
  2. Case-insensitive match
  3. Normalized match (strip separators, lowercase)
  4. Abbreviation expansion
  5. Fuzzy match (SequenceMatcher, threshold ≥ 0.7)
  6. Data-type compatibility as tiebreaker

When the target table does not exist, applies a naming convention
transform (snake_case → PascalCase by default) to generate SQL column names.
"""

import logging
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional

import pyarrow as pa

logger = logging.getLogger(__name__)

# =====================================================================
# Common abbreviation dictionary  (source abbrev → expanded forms)
# =====================================================================

ABBREVIATIONS: dict[str, list[str]] = {
    "id": ["identifier", "id"],
    "num": ["number", "num"],
    "no": ["number"],
    "nr": ["number"],
    "qty": ["quantity"],
    "amt": ["amount"],
    "val": ["value"],
    "desc": ["description"],
    "dt": ["date", "datetime"],
    "ts": ["timestamp"],
    "tm": ["time"],
    "yr": ["year"],
    "mo": ["month"],
    "addr": ["address"],
    "st": ["street", "state"],
    "cty": ["city"],
    "ctry": ["country"],
    "zip": ["zipcode", "postalcode"],
    "tel": ["telephone", "phone"],
    "ph": ["phone"],
    "mob": ["mobile"],
    "fax": ["fax"],
    "em": ["email"],
    "msg": ["message"],
    "cust": ["customer"],
    "cust_id": ["customerid"],
    "usr": ["user"],
    "emp": ["employee"],
    "mgr": ["manager"],
    "dept": ["department"],
    "org": ["organization", "organisation"],
    "co": ["company"],
    "prod": ["product"],
    "cat": ["category"],
    "inv": ["invoice", "inventory"],
    "ord": ["order"],
    "txn": ["transaction"],
    "tx": ["transaction"],
    "acct": ["account"],
    "bal": ["balance"],
    "pmt": ["payment"],
    "curr": ["currency"],
    "ccy": ["currency"],
    "px": ["price"],
    "prc": ["price"],
    "disc": ["discount"],
    "tot": ["total"],
    "sub": ["subtotal"],
    "src": ["source"],
    "dst": ["destination"],
    "dest": ["destination"],
    "tgt": ["target"],
    "ref": ["reference"],
    "stat": ["status"],
    "sts": ["status"],
    "flg": ["flag"],
    "ind": ["indicator"],
    "cnt": ["count"],
    "avg": ["average"],
    "min": ["minimum"],
    "max": ["maximum"],
    "pct": ["percent", "percentage"],
    "perc": ["percent", "percentage"],
    "lvl": ["level"],
    "grp": ["group"],
    "typ": ["type"],
    "nm": ["name"],
    "fn": ["firstname"],
    "ln": ["lastname"],
    "lname": ["lastname"],
    "fname": ["firstname"],
    "mname": ["middlename"],
    "dob": ["dateofbirth", "birthdate"],
    "img": ["image"],
    "pic": ["picture"],
    "doc": ["document"],
    "loc": ["location"],
    "lat": ["latitude"],
    "lon": ["longitude"],
    "lng": ["longitude"],
    "geo": ["geographic", "geography"],
    "cfg": ["config", "configuration"],
    "seq": ["sequence"],
    "ver": ["version"],
    "upd": ["updated", "update"],
    "crt": ["created", "create"],
    "del": ["deleted", "delete"],
    "mod": ["modified", "modify"],
    "ins": ["inserted", "insert"],
    "env": ["environment"],
    "srv": ["server", "service"],
    "svc": ["service"],
    "db": ["database"],
    "tbl": ["table"],
    "col": ["column"],
    "idx": ["index"],
    "pk": ["primarykey"],
    "fk": ["foreignkey"],
}

# Build reverse map: expanded form → abbreviation(s)
_REVERSE_ABBREV: dict[str, list[str]] = {}
for _abbr, _expansions in ABBREVIATIONS.items():
    for _exp in _expansions:
        _REVERSE_ABBREV.setdefault(_exp, []).append(_abbr)


# =====================================================================
# Naming convention transforms
# =====================================================================


def to_pascal_case(name: str) -> str:
    """snake_case / kebab-case → PascalCase."""
    return "".join(word.capitalize() for word in re.split(r"[_\-\s]+", name))


def to_camel_case(name: str) -> str:
    """snake_case / kebab-case → camelCase."""
    parts = re.split(r"[_\-\s]+", name)
    return parts[0].lower() + "".join(w.capitalize() for w in parts[1:])


def to_upper_snake(name: str) -> str:
    """any_case → UPPER_SNAKE_CASE."""
    # Insert underscore before uppercase letters in PascalCase/camelCase
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return re.sub(r"[\-\s]+", "_", s2).upper()


def to_lower_snake(name: str) -> str:
    """any_case → lower_snake_case."""
    return to_upper_snake(name).lower()


NAMING_TRANSFORMS = {
    "PascalCase": to_pascal_case,
    "camelCase": to_camel_case,
    "UPPER_SNAKE": to_upper_snake,
    "lower_snake": to_lower_snake,
    "as_is": lambda x: x,
}


# =====================================================================
# Normalization helpers
# =====================================================================


def _normalize(name: str) -> str:
    """Strip separators and lowercase for comparison."""
    return re.sub(r"[_\-\s]+", "", name).lower()


def _tokenize(name: str) -> list[str]:
    """Split a column name into word tokens."""
    # Handle PascalCase / camelCase
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return [t.lower() for t in re.split(r"[_\-\s]+", s) if t]


def _expand_tokens(tokens: list[str]) -> list[str]:
    """Expand abbreviated tokens using the abbreviation dictionary."""
    expanded = []
    for t in tokens:
        if t in ABBREVIATIONS:
            expanded.append(ABBREVIATIONS[t][0])  # primary expansion
        else:
            expanded.append(t)
    return expanded


def _contract_tokens(tokens: list[str]) -> list[str]:
    """Contract tokens to their abbreviated forms if known."""
    contracted = []
    for t in tokens:
        if t in _REVERSE_ABBREV:
            contracted.append(_REVERSE_ABBREV[t][0])
        else:
            contracted.append(t)
    return contracted


# =====================================================================
# Arrow ↔ SQL type compatibility
# =====================================================================

_TYPE_GROUPS = {
    "integer": {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64",
                "TINYINT", "SMALLINT", "INT", "BIGINT"},
    "float": {"float16", "float32", "float64", "float", "double",
              "REAL", "FLOAT", "NUMERIC", "DECIMAL"},
    "string": {"string", "large_string", "utf8", "large_utf8",
               "NVARCHAR", "VARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT"},
    "date": {"date32", "date64", "DATE"},
    "timestamp": {"timestamp", "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET"},
    "binary": {"binary", "large_binary", "VARBINARY", "BINARY", "IMAGE"},
    "boolean": {"bool", "BIT"},
}

_TYPE_TO_GROUP: dict[str, str] = {}
for _group, _types in _TYPE_GROUPS.items():
    for _t in _types:
        _TYPE_TO_GROUP[_t.lower()] = _group


def _type_compatible(arrow_type_str: str, sql_type_str: str) -> bool:
    """Check if an Arrow type and a SQL Server type are in the same group."""
    # Extract base type (strip precision/length/tz info)
    a_base = re.split(r"[\[\(]", arrow_type_str)[0].strip().lower()
    s_base = re.split(r"[\[\(]", sql_type_str)[0].strip().lower()
    a_group = _TYPE_TO_GROUP.get(a_base)
    s_group = _TYPE_TO_GROUP.get(s_base)
    if a_group and s_group:
        return a_group == s_group
    return False  # unknown types are not considered compatible


# =====================================================================
# Match result
# =====================================================================


@dataclass
class ColumnMatch:
    source_name: str
    target_name: str
    strategy: str  # which layer matched
    confidence: float  # 0.0 – 1.0
    source_type: str = ""
    target_type: str = ""


@dataclass
class MappingResult:
    """Complete mapping result for a table."""
    matched: list[ColumnMatch] = field(default_factory=list)
    unmatched_source: list[str] = field(default_factory=list)  # Iceberg cols with no target
    unmatched_target: list[str] = field(default_factory=list)  # SQL cols with no source

    def source_to_target(self) -> dict[str, str]:
        """Return a {source_col: target_col} dict for matched columns."""
        return {m.source_name: m.target_name for m in self.matched}

    def log_summary(self) -> None:
        logger.info("Column mapping results:")
        logger.info("  %-30s  ->  %-30s  %-15s  %.0f%%", "SOURCE", "TARGET", "STRATEGY", 0)
        logger.info("  " + "-" * 90)
        for m in self.matched:
            logger.info(
                "  %-30s  ->  %-30s  %-15s  %.0f%%",
                m.source_name, m.target_name, m.strategy, m.confidence * 100,
            )
        if self.unmatched_source:
            logger.warning("  Unmatched source columns (will be skipped): %s", self.unmatched_source)
        if self.unmatched_target:
            logger.warning("  Unmatched target columns (will be NULL): %s", self.unmatched_target)


# =====================================================================
# Core mapper
# =====================================================================

FUZZY_THRESHOLD = 0.70


class ColumnMapper:
    """
    Automatically maps source (Iceberg/Arrow) columns to target (SQL Server) columns.

    Usage:
        mapper = ColumnMapper(naming_convention="PascalCase")

        # If target table exists — pass its columns:
        result = mapper.auto_map(arrow_schema, target_columns)

        # If target table doesn't exist — generate target names:
        target_names = mapper.transform_names(arrow_schema)
    """

    def __init__(self, naming_convention: str = "PascalCase") -> None:
        self.naming_convention = naming_convention
        self._transform = NAMING_TRANSFORMS.get(naming_convention, to_pascal_case)

    # ------------------------------------------------------------------
    # Generate target column names (table doesn't exist yet)
    # ------------------------------------------------------------------

    def transform_names(self, arrow_schema: pa.Schema) -> list[str]:
        """Apply the naming convention to create target column names."""
        return [self._transform(f.name) for f in arrow_schema]

    def transform_schema(self, arrow_schema: pa.Schema) -> pa.Schema:
        """Return a new Arrow schema with transformed column names."""
        new_names = self.transform_names(arrow_schema)
        return pa.schema([
            pa.field(new_name, f.type, nullable=f.nullable)
            for new_name, f in zip(new_names, arrow_schema)
        ])

    # ------------------------------------------------------------------
    # Auto-map against existing target columns
    # ------------------------------------------------------------------

    def auto_map(
        self,
        arrow_schema: pa.Schema,
        target_columns: list[dict],  # [{"name": "Col", "type": "INT", ...}]
    ) -> MappingResult:
        """
        Match source columns to target columns using multi-layer strategy.

        Parameters
        ----------
        arrow_schema : pa.Schema
            Source Arrow schema from Iceberg.
        target_columns : list[dict]
            Each dict has at least ``name`` and ``type`` keys, obtained from
            SQL Server INFORMATION_SCHEMA.COLUMNS.

        Returns
        -------
        MappingResult
        """
        result = MappingResult()
        source_cols = [(f.name, str(f.type)) for f in arrow_schema]
        target_pool = {tc["name"]: tc for tc in target_columns}  # mutable copy

        # Pre-compute normalized forms for targets
        target_normalized = {
            name: _normalize(name) for name in target_pool
        }
        target_tokens = {
            name: _tokenize(name) for name in target_pool
        }
        target_expanded = {
            name: "".join(_expand_tokens(_tokenize(name))) for name in target_pool
        }

        used_targets: set[str] = set()

        for src_name, src_type in source_cols:
            match = self._find_best_match(
                src_name, src_type,
                target_pool, target_normalized, target_tokens, target_expanded,
                used_targets,
            )
            if match:
                result.matched.append(match)
                used_targets.add(match.target_name)
            else:
                result.unmatched_source.append(src_name)

        # Remaining unmatched targets
        result.unmatched_target = [
            name for name in target_pool if name not in used_targets
        ]

        return result

    def _find_best_match(
        self,
        src_name: str,
        src_type: str,
        target_pool: dict[str, dict],
        target_normalized: dict[str, str],
        target_tokens: dict[str, list[str]],
        target_expanded: dict[str, str],
        used: set[str],
    ) -> Optional[ColumnMatch]:
        """Try each matching layer in priority order."""
        available = {n: t for n, t in target_pool.items() if n not in used}
        if not available:
            return None

        # --- Layer 1: Exact match ---
        if src_name in available:
            return ColumnMatch(src_name, src_name, "exact", 1.0, src_type,
                               available[src_name].get("type", ""))

        # --- Layer 2: Case-insensitive ---
        src_lower = src_name.lower()
        for tgt_name in available:
            if tgt_name.lower() == src_lower:
                return ColumnMatch(src_name, tgt_name, "case-insensitive", 0.98,
                                   src_type, available[tgt_name].get("type", ""))

        # --- Layer 3: Normalized match (strip separators) ---
        src_norm = _normalize(src_name)
        for tgt_name in available:
            if target_normalized[tgt_name] == src_norm:
                return ColumnMatch(src_name, tgt_name, "normalized", 0.95,
                                   src_type, available[tgt_name].get("type", ""))

        # --- Layer 4: Abbreviation expansion ---
        src_tokens = _tokenize(src_name)
        src_expanded = "".join(_expand_tokens(src_tokens))
        src_contracted = "".join(_contract_tokens(src_tokens))

        for tgt_name in available:
            tgt_exp = target_expanded[tgt_name]
            # Compare expanded forms
            if src_expanded == tgt_exp:
                return ColumnMatch(src_name, tgt_name, "abbreviation", 0.90,
                                   src_type, available[tgt_name].get("type", ""))
            # Compare contracted source against normalized target
            if src_contracted == target_normalized[tgt_name]:
                return ColumnMatch(src_name, tgt_name, "abbreviation", 0.88,
                                   src_type, available[tgt_name].get("type", ""))
            # Compare source normalized against contracted target
            tgt_contracted = "".join(_contract_tokens(_tokenize(tgt_name)))
            if src_norm == tgt_contracted:
                return ColumnMatch(src_name, tgt_name, "abbreviation", 0.88,
                                   src_type, available[tgt_name].get("type", ""))

        # --- Layer 5: Fuzzy match ---
        best_ratio = 0.0
        best_target: Optional[str] = None
        for tgt_name in available:
            # Compare normalized forms for better fuzzy results
            ratio = SequenceMatcher(
                None, src_expanded, target_expanded[tgt_name]
            ).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_target = tgt_name

        if best_target and best_ratio >= FUZZY_THRESHOLD:
            # --- Layer 6: Type compatibility as confidence booster ---
            tgt_type = available[best_target].get("type", "")
            type_ok = _type_compatible(src_type, tgt_type)
            confidence = best_ratio
            if type_ok:
                confidence = min(confidence + 0.05, 0.99)
            strategy = "fuzzy" + ("+type" if type_ok else "")

            return ColumnMatch(src_name, best_target, strategy, confidence,
                               src_type, tgt_type)

        return None

    # ------------------------------------------------------------------
    # Reverse mapping (SQL Server → Iceberg)
    # ------------------------------------------------------------------

    def reverse_map(
        self,
        sql_columns: list[dict],  # [{"name": "Col", "type": "INT", ...}]
        iceberg_schema: pa.Schema,
    ) -> MappingResult:
        """
        Reverse mapping: Map SQL Server columns back to Iceberg column names.
        
        Used in ingestion flow when reading from SQL Server and writing to Iceberg.
        
        Parameters
        ----------
        sql_columns : list[dict]
            SQL Server columns, each with at least ``name`` and ``type`` keys.
        iceberg_schema : pa.Schema
            Target Iceberg Arrow schema to map to.
        
        Returns
        -------
        MappingResult
            Mapping from SQL names to Iceberg names.
        """
        result = MappingResult()
        source_cols = [(col["name"], col.get("type", "")) for col in sql_columns]
        target_pool = {f.name: {"name": f.name, "type": str(f.type)} for f in iceberg_schema}

        # Pre-compute normalized forms for targets
        target_normalized = {
            name: _normalize(name) for name in target_pool
        }
        target_tokens = {
            name: _tokenize(name) for name in target_pool
        }
        target_expanded = {
            name: "".join(_expand_tokens(_tokenize(name))) for name in target_pool
        }

        used_targets: set[str] = set()

        for src_name, src_type in source_cols:
            match = self._find_best_match(
                src_name, src_type,
                target_pool, target_normalized, target_tokens, target_expanded,
                used_targets,
            )
            if match:
                result.matched.append(match)
                used_targets.add(match.target_name)
            else:
                result.unmatched_source.append(src_name)

        # Remaining unmatched targets
        result.unmatched_target = [
            name for name in target_pool if name not in used_targets
        ]

        return result

    def reverse_transform_names(self, sql_columns: list[str]) -> list[str]:
        """
        Transform SQL Server column names to Iceberg-style names.
        
        Applies reverse naming convention (e.g., PascalCase → snake_case).
        
        Parameters
        ----------
        sql_columns : list[str]
            List of SQL Server column names.
        
        Returns
        -------
        list[str]
            Transformed column names for Iceberg.
        """
        # For ingestion, we typically want snake_case for Iceberg
        return [to_lower_snake(col) for col in sql_columns]

    def invert_mapping(self, mapping_result: MappingResult) -> MappingResult:
        """
        Invert a mapping result (swap source and target).
        
        Useful when you have an extraction mapping and need the ingestion mapping.
        
        Parameters
        ----------
        mapping_result : MappingResult
            Original mapping (e.g., Iceberg → SQL).
        
        Returns
        -------
        MappingResult
            Inverted mapping (e.g., SQL → Iceberg).
        """
        inverted = MappingResult()
        
        for match in mapping_result.matched:
            inverted.matched.append(ColumnMatch(
                source_name=match.target_name,
                target_name=match.source_name,
                strategy=f"inverted_{match.strategy}",
                confidence=match.confidence,
                source_type=match.target_type,
                target_type=match.source_type,
            ))
        
        # Swap unmatched lists
        inverted.unmatched_source = mapping_result.unmatched_target
        inverted.unmatched_target = mapping_result.unmatched_source
        
        return inverted
