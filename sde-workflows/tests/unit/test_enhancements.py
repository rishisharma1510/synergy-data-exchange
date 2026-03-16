"""
Tests for SID Optional Enhancements

Tests checkpoint/resume, parallel processing, and audit trail functionality.
"""

import json
import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict

import pyarrow as pa

from src.sid.checkpoint import (
    CheckpointState,
    LocalCheckpointManager,
    S3CheckpointManager,
    create_checkpoint_manager,
)
from src.sid.audit_trail import (
    AllocationRecord,
    TableAllocationBatch,
    LocalAuditTrail,
    AuditTrailCollector,
    create_audit_trail,
)
from src.sid.parallel import (
    ParallelTransformer,
    LevelResult,
    ParallelJobResult,
    create_parallel_transformer,
)
from src.sid.enhanced import EnhancedTransformer, EnhancedJobResult
from src.sid.transformer import create_transformer


# ============================================================================
# Checkpoint Tests
# ============================================================================

class TestCheckpointState:
    """Tests for CheckpointState dataclass."""
    
    def test_mark_table_complete(self):
        """Test marking a table as complete with mapping."""
        state = CheckpointState(job_id="job-1", tenant_id="tenant-1")
        
        mapping = {100: 1000, 101: 1001, 102: 1002}
        state.mark_table_complete("tCompany", mapping)
        
        assert "tCompany" in state.completed_tables
        assert state.is_table_completed("tCompany")
        assert not state.is_table_completed("tContract")
    
    def test_get_mapping_converts_keys(self):
        """Test that mapping keys are converted from string to int."""
        state = CheckpointState(job_id="job-1", tenant_id="tenant-1")
        
        # Store mapping (converts to string keys for JSON)
        original_mapping = {100: 1000, 101: 1001}
        state.mark_table_complete("tCompany", original_mapping)
        
        # Retrieve mapping (should convert back to int keys)
        retrieved = state.get_mapping("tCompany")
        assert retrieved == {100: 1000, 101: 1001}
        assert all(isinstance(k, int) for k in retrieved.keys())
    
    def test_status_transitions(self):
        """Test job status transitions."""
        state = CheckpointState(job_id="job-1", tenant_id="tenant-1")
        
        assert state.status == "in_progress"
        
        state.mark_completed()
        assert state.status == "completed"
        
        state2 = CheckpointState(job_id="job-2", tenant_id="tenant-1")
        state2.mark_failed("Test error")
        assert state2.status == "failed"
        assert state2.error == "Test error"
    
    def test_serialization_roundtrip(self):
        """Test to_dict/from_dict roundtrip."""
        state = CheckpointState(job_id="job-1", tenant_id="tenant-1")
        state.mark_table_complete("tCompany", {100: 1000})
        state.set_current_table("tContract")
        
        data = state.to_dict()
        restored = CheckpointState.from_dict(data)
        
        assert restored.job_id == "job-1"
        assert restored.tenant_id == "tenant-1"
        assert "tCompany" in restored.completed_tables
        assert restored.current_table == "tContract"


class TestLocalCheckpointManager:
    """Tests for LocalCheckpointManager."""
    
    def test_save_and_load(self):
        """Test saving and loading checkpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = LocalCheckpointManager(checkpoint_dir=tmpdir)
            
            state = CheckpointState(job_id="job-123", tenant_id="tenant-456")
            state.mark_table_complete("tCompany", {1: 1000, 2: 1001})
            
            manager.save(state)
            
            loaded = manager.load("job-123", "tenant-456")
            
            assert loaded is not None
            assert loaded.job_id == "job-123"
            assert "tCompany" in loaded.completed_tables
    
    def test_load_nonexistent_returns_none(self):
        """Test loading non-existent checkpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = LocalCheckpointManager(checkpoint_dir=tmpdir)
            
            loaded = manager.load("nonexistent", "tenant")
            assert loaded is None
    
    def test_delete_checkpoint(self):
        """Test deleting checkpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = LocalCheckpointManager(checkpoint_dir=tmpdir)
            
            state = CheckpointState(job_id="job-123", tenant_id="tenant-456")
            manager.save(state)
            
            assert manager.exists("job-123", "tenant-456")
            
            manager.delete("job-123", "tenant-456")
            
            assert not manager.exists("job-123", "tenant-456")
    
    def test_exists(self):
        """Test checkpoint existence check."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = LocalCheckpointManager(checkpoint_dir=tmpdir)
            
            assert not manager.exists("job-123", "tenant-456")
            
            state = CheckpointState(job_id="job-123", tenant_id="tenant-456")
            manager.save(state)
            
            assert manager.exists("job-123", "tenant-456")


class TestCreateCheckpointManager:
    """Tests for checkpoint manager factory."""
    
    def test_creates_local_by_default(self):
        """Test factory creates local manager by default."""
        manager = create_checkpoint_manager()
        assert isinstance(manager, LocalCheckpointManager)
    
    def test_creates_s3_when_requested(self):
        """Test factory creates S3 manager when requested."""
        manager = create_checkpoint_manager(
            use_s3=True,
            bucket="test-bucket",
        )
        assert isinstance(manager, S3CheckpointManager)
    
    def test_s3_requires_bucket(self):
        """Test S3 manager requires bucket."""
        with pytest.raises(ValueError, match="S3 bucket required"):
            create_checkpoint_manager(use_s3=True, bucket="")


# ============================================================================
# Audit Trail Tests
# ============================================================================

class TestAllocationRecord:
    """Tests for AllocationRecord dataclass."""
    
    def test_to_dict(self):
        """Test serialization to dict."""
        record = AllocationRecord(
            table_name="tCompany",
            column_name="CompanySID",
            original_id=100,
            new_sid=1000,
            job_id="job-1",
            tenant_id="tenant-1",
        )
        
        data = record.to_dict()
        
        assert data["table_name"] == "tCompany"
        assert data["original_id"] == 100
        assert data["new_sid"] == 1000
        assert "timestamp_iso" in data


class TestTableAllocationBatch:
    """Tests for TableAllocationBatch."""
    
    def test_from_mapping(self):
        """Test creating batch from SID mapping."""
        mapping = {100: 1000, 101: 1001, 102: 1002}
        
        batch = TableAllocationBatch.from_mapping(
            table_name="tCompany",
            column_name="CompanySID",
            mapping=mapping,
            job_id="job-1",
            tenant_id="tenant-1",
        )
        
        assert batch.table_name == "tCompany"
        assert len(batch.records) == 3
        assert batch.sid_range_start == 1000
        assert batch.sid_range_end == 1002
    
    def test_to_dict(self):
        """Test serialization to dict."""
        mapping = {100: 1000, 101: 1001}
        batch = TableAllocationBatch.from_mapping(
            table_name="tCompany",
            column_name="CompanySID",
            mapping=mapping,
            job_id="job-1",
            tenant_id="tenant-1",
        )
        
        data = batch.to_dict()
        
        assert data["table_name"] == "tCompany"
        assert data["record_count"] == 2
        assert len(data["records"]) == 2


class TestLocalAuditTrail:
    """Tests for LocalAuditTrail backend."""
    
    def test_write_and_query_batch(self):
        """Test writing and querying batch."""
        with tempfile.TemporaryDirectory() as tmpdir:
            audit = LocalAuditTrail(audit_dir=tmpdir)
            
            batch = TableAllocationBatch.from_mapping(
                table_name="tCompany",
                column_name="CompanySID",
                mapping={100: 1000, 101: 1001},
                job_id="job-1",
                tenant_id="tenant-1",
            )
            
            audit.write_batch(batch)
            
            # Query by job
            batches = audit.query_by_job("job-1")
            assert len(batches) == 1
            assert batches[0].table_name == "tCompany"
    
    def test_query_by_original_id(self):
        """Test querying by original ID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            audit = LocalAuditTrail(audit_dir=tmpdir)
            
            batch = TableAllocationBatch.from_mapping(
                table_name="tCompany",
                column_name="CompanySID",
                mapping={100: 1000, 101: 1001},
                job_id="job-1",
                tenant_id="tenant-1",
            )
            audit.write_batch(batch)
            
            records = audit.query_by_original_id("tCompany", 100)
            assert len(records) == 1
            assert records[0].new_sid == 1000
    
    def test_query_by_sid(self):
        """Test querying by new SID."""
        with tempfile.TemporaryDirectory() as tmpdir:
            audit = LocalAuditTrail(audit_dir=tmpdir)
            
            batch = TableAllocationBatch.from_mapping(
                table_name="tCompany",
                column_name="CompanySID",
                mapping={100: 1000, 101: 1001},
                job_id="job-1",
                tenant_id="tenant-1",
            )
            audit.write_batch(batch)
            
            record = audit.query_by_sid("tCompany", 1000)
            assert record is not None
            assert record.original_id == 100


class TestAuditTrailCollector:
    """Tests for AuditTrailCollector."""
    
    def test_record_allocation(self):
        """Test recording allocations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalAuditTrail(audit_dir=tmpdir)
            
            collector = AuditTrailCollector(
                backends=[backend],
                job_id="job-1",
                tenant_id="tenant-1",
            )
            
            collector.record_allocation(
                table_name="tCompany",
                column_name="CompanySID",
                mapping={100: 1000, 101: 1001},
            )
            
            assert collector.batches_written == 1
            assert collector.records_written == 2
    
    def test_empty_mapping_skipped(self):
        """Test that empty mapping is skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backend = LocalAuditTrail(audit_dir=tmpdir)
            
            collector = AuditTrailCollector(
                backends=[backend],
                job_id="job-1",
                tenant_id="tenant-1",
            )
            
            collector.record_allocation(
                table_name="tCompany",
                column_name="CompanySID",
                mapping={},
            )
            
            assert collector.batches_written == 0


# ============================================================================
# Parallel Processing Tests
# ============================================================================

class TestParallelTransformer:
    """Tests for ParallelTransformer."""
    
    def _create_test_tables(self) -> Dict[str, pa.Table]:
        """Create test PyArrow tables."""
        return {
            "tCompany": pa.table({
                "CompanySID": [1, 2, 3],
                "Name": ["A", "B", "C"],
            }),
            "tExposureSet": pa.table({
                "ExposureSetSID": [10, 20],
                "CompanySID": [1, 2],
            }),
            "tContract": pa.table({
                "ContractSID": [100, 200],
                "ExposureSetSID": [10, 20],
            }),
        }
    
    def test_get_tables_by_level(self):
        """Test grouping tables by level."""
        parallel = create_parallel_transformer(use_mock=True, max_workers=2)
        tables = self._create_test_tables()
        
        by_level = parallel.get_tables_by_level(tables)
        
        assert 0 in by_level  # tCompany
        assert 1 in by_level  # tExposureSet
        assert 2 in by_level  # tContract
    
    def test_transform_level(self):
        """Test transforming a single level."""
        parallel = create_parallel_transformer(use_mock=True, max_workers=2)
        tables = self._create_test_tables()
        
        result = parallel.transform_level(
            level=0,
            tables=tables,
            table_names=["tCompany"],
        )
        
        assert isinstance(result, LevelResult)
        assert result.level == 0
        assert result.tables_processed == 1
        assert result.success
    
    def test_transform_all_parallel(self):
        """Test full parallel transformation."""
        parallel = create_parallel_transformer(use_mock=True, max_workers=2)
        tables = self._create_test_tables()
        
        result = parallel.transform_all_parallel(tables)
        
        assert isinstance(result, ParallelJobResult)
        assert result.tables_processed == 3
        assert result.success
    
    def test_callbacks_called(self):
        """Test that callbacks are called."""
        parallel = create_parallel_transformer(use_mock=True, max_workers=2)
        tables = self._create_test_tables()
        
        table_complete_count = [0]
        level_complete_count = [0]
        
        def on_table(result):
            table_complete_count[0] += 1
        
        def on_level(result):
            level_complete_count[0] += 1
        
        parallel.transform_all_parallel(
            tables,
            on_table_complete=on_table,
            on_level_complete=on_level,
        )
        
        assert table_complete_count[0] == 3
        assert level_complete_count[0] == 3  # 3 levels
    
    def test_speedup_estimate(self):
        """Test speedup estimation."""
        parallel = create_parallel_transformer(use_mock=True, max_workers=4)
        tables = self._create_test_tables()
        
        speedup, explanation = parallel.get_speedup_estimate(tables)
        
        assert speedup >= 1.0
        assert "Workers" in explanation


# ============================================================================
# Enhanced Transformer Tests
# ============================================================================

class TestEnhancedTransformer:
    """Tests for EnhancedTransformer."""
    
    def _create_test_tables(self) -> Dict[str, pa.Table]:
        """Create test PyArrow tables."""
        return {
            "tCompany": pa.table({
                "CompanySID": [1, 2, 3],
                "Name": ["A", "B", "C"],
            }),
            "tExposureSet": pa.table({
                "ExposureSetSID": [10, 20],
                "CompanySID": [1, 2],
            }),
        }
    
    def test_create_factory(self):
        """Test factory method creates transformer."""
        transformer = EnhancedTransformer.create(
            job_id="job-1",
            tenant_id="tenant-1",
            use_checkpoint=True,
            use_parallel=True,
            use_audit_trail=True,
            use_mock_sid=True,
        )
        
        assert transformer is not None
    
    def test_transform_all_sequential(self):
        """Test transformation without parallel processing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            transformer = EnhancedTransformer.create(
                job_id="job-1",
                tenant_id="tenant-1",
                use_checkpoint=False,
                use_parallel=False,
                use_audit_trail=False,
                use_mock_sid=True,
            )
            
            tables = self._create_test_tables()
            result = transformer.transform_all(tables)
            
            assert isinstance(result, EnhancedJobResult)
            assert result.success
            assert result.tables_processed >= 1
    
    def test_transform_all_parallel(self):
        """Test transformation with parallel processing."""
        transformer = EnhancedTransformer.create(
            job_id="job-1",
            tenant_id="tenant-1",
            use_checkpoint=False,
            use_parallel=True,
            use_audit_trail=False,
            use_mock_sid=True,
            max_workers=2,
        )
        
        tables = self._create_test_tables()
        result = transformer.transform_all(tables)
        
        assert result.success
        assert result.tables_processed >= 1
    
    def test_checkpoint_resume(self):
        """Test checkpoint and resume functionality."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First run - process some tables
            transformer1 = EnhancedTransformer.create(
                job_id="job-resume",
                tenant_id="tenant-1",
                use_checkpoint=True,
                use_parallel=False,
                use_audit_trail=False,
                use_mock_sid=True,
            )
            transformer1._checkpoint_mgr = LocalCheckpointManager(tmpdir)
            
            # Manually create a checkpoint as if interrupted
            state = CheckpointState(job_id="job-resume", tenant_id="tenant-1")
            state.mark_table_complete("tCompany", {1: 1000, 2: 1001, 3: 1002})
            transformer1._checkpoint_mgr.save(state)
            
            # Second run - should resume
            transformer2 = EnhancedTransformer.create(
                job_id="job-resume",
                tenant_id="tenant-1",
                use_checkpoint=True,
                use_parallel=False,
                use_audit_trail=False,
                use_mock_sid=True,
            )
            transformer2._checkpoint_mgr = LocalCheckpointManager(tmpdir)
            
            tables = self._create_test_tables()
            result = transformer2.transform_all(tables)
            
            assert result.resumed_from_checkpoint
            assert result.tables_skipped >= 1
    
    def test_audit_trail_written(self):
        """Test audit trail records are written."""
        with tempfile.TemporaryDirectory() as tmpdir:
            transformer = EnhancedTransformer.create(
                job_id="job-audit",
                tenant_id="tenant-1",
                use_checkpoint=False,
                use_parallel=False,
                use_audit_trail=True,
                use_mock_sid=True,
            )
            
            tables = self._create_test_tables()
            result = transformer.transform_all(tables)
            
            assert result.audit_records_written > 0
    
    def test_get_transformed_tables(self):
        """Test retrieving transformed tables."""
        transformer = EnhancedTransformer.create(
            job_id="job-1",
            tenant_id="tenant-1",
            use_checkpoint=False,
            use_parallel=False,
            use_audit_trail=False,
            use_mock_sid=True,
        )
        
        tables = self._create_test_tables()
        transformer.transform_all(tables)
        
        transformed = transformer.get_transformed_tables()
        assert len(transformed) >= 1
    
    def test_get_mapping(self):
        """Test retrieving SID mapping for a table."""
        transformer = EnhancedTransformer.create(
            job_id="job-1",
            tenant_id="tenant-1",
            use_checkpoint=False,
            use_parallel=False,
            use_audit_trail=False,
            use_mock_sid=True,
        )
        
        tables = self._create_test_tables()
        transformer.transform_all(tables)
        
        mapping = transformer.get_mapping("tCompany")
        assert len(mapping) == 3  # 3 rows in tCompany
    
    def test_progress_callback(self):
        """Test progress callback is called."""
        transformer = EnhancedTransformer.create(
            job_id="job-1",
            tenant_id="tenant-1",
            use_checkpoint=False,
            use_parallel=False,
            use_audit_trail=False,
            use_mock_sid=True,
        )
        
        progress_count = [0]
        
        def on_progress(result):
            progress_count[0] += 1
        
        tables = self._create_test_tables()
        transformer.transform_all(tables, on_progress=on_progress)
        
        assert progress_count[0] >= 1


class TestEnhancedJobResult:
    """Tests for EnhancedJobResult."""
    
    def test_to_dict(self):
        """Test serialization to dict."""
        result = EnhancedJobResult(
            job_id="job-1",
            tenant_id="tenant-1",
            tables_processed=5,
            tables_skipped=2,
            total_rows=1000,
            duration_ms=5000.5,
            success=True,
            resumed_from_checkpoint=True,
            audit_records_written=100,
        )
        
        data = result.to_dict()
        
        assert data["job_id"] == "job-1"
        assert data["tables_processed"] == 5
        assert data["tables_skipped"] == 2
        assert data["resumed_from_checkpoint"] is True
        assert data["audit_records_written"] == 100
