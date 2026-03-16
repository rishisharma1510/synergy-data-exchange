"""
Unit tests for execution_plan models.
"""

import pytest
from datetime import datetime
from src.models import (
    ExecutionPlan,
    TablePlan,
    ColumnPlan,
    ArtifactType,
    LoadMode,
    build_execution_plan,
)


class TestTablePlan:
    """Tests for TablePlan model."""
    
    def test_default_values(self):
        plan = TablePlan(
            name="test_table",
            source_namespace="default",
            source_table="test_table",
            target_table="test_table",
        )
        
        assert plan.target_schema == "dbo"
        assert plan.load_mode == LoadMode.FULL
        assert plan.create_if_missing is True
        assert plan.row_filter is None
        assert plan.columns == []
    
    def test_with_row_filter(self):
        plan = TablePlan(
            name="filtered_table",
            source_namespace="default",
            source_table="source",
            target_table="target",
            row_filter="exposure_id IN (1, 2, 3)",
        )
        
        assert plan.row_filter == "exposure_id IN (1, 2, 3)"


class TestExecutionPlan:
    """Tests for ExecutionPlan model."""
    
    def test_total_tables(self):
        plan = ExecutionPlan(
            tenant_id="tenant1",
            run_id="run1",
            tables=[
                TablePlan(name="t1", source_namespace="ns", source_table="t1", target_table="t1"),
                TablePlan(name="t2", source_namespace="ns", source_table="t2", target_table="t2"),
            ],
        )
        
        assert plan.total_tables == 2
    
    def test_artifact_type_default(self):
        plan = ExecutionPlan(
            tenant_id="tenant1",
            run_id="run1",
        )
        
        assert plan.artifact_type == ArtifactType.BAK


class TestBuildExecutionPlan:
    """Tests for build_execution_plan function."""
    
    def test_basic_build(self):
        tables = ["ns.table1", "ns.table2"]
        overrides = {}
        exposure_ids = [1, 2, 3]
        
        plan = build_execution_plan(
            tables=tables,
            overrides=overrides,
            exposure_ids=exposure_ids,
            tenant_id="test_tenant",
            run_id="test_run",
        )
        
        assert plan.tenant_id == "test_tenant"
        assert plan.run_id == "test_run"
        assert plan.exposure_ids == [1, 2, 3]
        assert len(plan.tables) == 2
    
    def test_with_overrides(self):
        tables = ["ns.table1"]
        overrides = {
            "ns_table1": {"target_table": "custom_name"}
        }
        
        plan = build_execution_plan(
            tables=tables,
            overrides=overrides,
            exposure_ids=[],
            tenant_id="test",
            run_id="run",
        )
        
        assert plan.tables[0].target_table == "custom_name"
