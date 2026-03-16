"""
Unit tests for column_mapper module.
"""

import pytest
import pyarrow as pa
from src.core.column_mapper import ColumnMapper, _normalize


class TestNormalizeColumnName:
    """Tests for column name normalization."""
    
    def test_removes_underscores(self):
        assert _normalize("first_name") == "firstname"
    
    def test_lowercases(self):
        assert _normalize("FirstName") == "firstname"
    
    def test_removes_spaces(self):
        assert _normalize("First Name") == "firstname"


class TestMatchColumns:
    """Tests for column matching via ColumnMapper."""
    
    def test_exact_match(self):
        source_schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])
        target_cols = [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "value", "type": "FLOAT"},
        ]
        
        mapper = ColumnMapper()
        result = mapper.auto_map(source_schema, target_cols)
        mapping = result.source_to_target()
        
        assert mapping["id"] == "id"
        assert mapping["name"] == "name"
        assert mapping["value"] == "value"
    
    def test_case_insensitive_match(self):
        source_schema = pa.schema([
            pa.field("ID", pa.int32()),
            pa.field("Name", pa.string()),
            pa.field("VALUE", pa.float64()),
        ])
        target_cols = [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "value", "type": "FLOAT"},
        ]
        
        mapper = ColumnMapper()
        result = mapper.auto_map(source_schema, target_cols)
        mapping = result.source_to_target()
        
        assert mapping["ID"] == "id"
        assert mapping["Name"] == "name"
        assert mapping["VALUE"] == "value"
    
    def test_normalized_match(self):
        source_schema = pa.schema([
            pa.field("FirstName", pa.string()),
            pa.field("LastName", pa.string()),
        ])
        target_cols = [
            {"name": "first_name", "type": "VARCHAR"},
            {"name": "last_name", "type": "VARCHAR"},
        ]
        
        mapper = ColumnMapper()
        result = mapper.auto_map(source_schema, target_cols)
        mapping = result.source_to_target()
        
        assert mapping["FirstName"] == "first_name"
        assert mapping["LastName"] == "last_name"


class TestMapColumns:
    """Tests for full column mapping with type inference."""
    
    def test_with_arrow_schema(self):
        source_schema = pa.schema([
            pa.field("user_id", pa.int64()),
            pa.field("user_name", pa.string()),
        ])
        
        mapper = ColumnMapper(naming_convention="PascalCase")
        target_names = mapper.transform_names(source_schema)
        
        assert "UserId" in target_names
        assert "UserName" in target_names
