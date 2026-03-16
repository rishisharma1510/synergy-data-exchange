"""
Unit tests for Exposure API Client

Tests the ExposureClient and related functions:
- MockExposureClient for local testing
- extract_exposure_metadata() function
- SID transformer override mapping integration
"""

import pytest
import pyarrow as pa

from src.sid.exposure_client import (
    ExposureMetadata,
    ExposureAllocationResponse,
    ExposureSetResult,
    MockExposureClient,
    ExposureClient,
    ExposureAPIError,
    extract_exposure_metadata,
    create_exposure_client,
)
from src.sid.transformer import SIDTransformer, create_transformer


class TestExposureMetadata:
    """Tests for ExposureMetadata dataclass."""
    
    def test_basic_metadata(self):
        metadata = ExposureMetadata(
            exposure_set_names=["Set A", "Set B"],
            exposure_set_count=2,
            exposure_view_names=["View A"],
            exposure_view_count=1,
            tenant_id="tenant-123",
            run_id="run-abc-def",
            source_artifact="backup.bak",
        )
        
        assert len(metadata.exposure_set_names) == 2
        assert metadata.exposure_set_count == 2
        assert metadata.tenant_id == "tenant-123"


class TestExposureAllocationResponse:
    """Tests for ExposureAllocationResponse dataclass."""

    def test_basic_response(self):
        response = ExposureAllocationResponse(
            exposure_sets=[
                ExposureSetResult(name="Set A", sid=1000),
                ExposureSetResult(name="Set B", sid=1001),
            ]
        )

        assert response.exposure_set_sid == 1000  # property: first SID
        assert response.exposure_set_sids == [1000, 1001]

    def test_single_set(self):
        response = ExposureAllocationResponse(
            exposure_sets=[ExposureSetResult(name="Only Set", sid=5000)]
        )

        assert response.exposure_set_sid == 5000
        assert len(response.exposure_sets) == 1

    def test_empty_sets_returns_none_sid(self):
        response = ExposureAllocationResponse(exposure_sets=[])

        assert response.exposure_set_sid is None
        assert response.exposure_set_sids == []


class TestMockExposureClient:
    """Tests for MockExposureClient."""
    
    @pytest.fixture
    def mock_client(self):
        return MockExposureClient(starting_sid=9_000_000)
    
    @pytest.fixture
    def sample_metadata(self):
        return ExposureMetadata(
            exposure_set_names=["Set A"],
            exposure_set_count=1,
            exposure_view_names=["View A"],
            exposure_view_count=1,
            tenant_id="tenant-123",
            run_id="run-456",
            source_artifact="test.bak",
        )
    
    def test_create_exposure_returns_incrementing_sids(self, mock_client, sample_metadata):
        """Mock client should return one SID per exposure set name."""
        response1 = mock_client.create_exposure(sample_metadata)  # 1 name → 1 SID
        response2 = mock_client.create_exposure(sample_metadata)

        assert response1.exposure_set_sid == 9_000_000
        assert len(response1.exposure_sets) == 1

        assert response2.exposure_set_sid == 9_000_001
        assert len(response2.exposure_sets) == 1

    def test_multiple_names_get_separate_sids(self, mock_client):
        """Each ExposureSet name should receive its own SID."""
        metadata = ExposureMetadata(
            exposure_set_names=["Set A", "Set B", "Set C"],
            exposure_set_count=3,
            exposure_view_names=[],
            exposure_view_count=0,
            tenant_id="tenant-123",
            run_id="run-456",
            source_artifact="test.bak",
        )

        response = mock_client.create_exposure(metadata)

        assert len(response.exposure_sets) == 3
        assert response.exposure_set_sids == [9_000_000, 9_000_001, 9_000_002]
        assert [r.name for r in response.exposure_sets] == ["Set A", "Set B", "Set C"]
    
    def test_health_check_returns_true(self, mock_client):
        """Mock client health check should always return True."""
        assert mock_client.health_check() is True
    
    def test_allocation_history_tracked(self, mock_client, sample_metadata):
        """Mock client should track allocation history."""
        mock_client.create_exposure(sample_metadata)
        mock_client.create_exposure(sample_metadata)
        
        assert len(mock_client.allocation_history) == 2


class TestExtractExposureMetadata:
    """Tests for extract_exposure_metadata function."""
    
    @pytest.fixture
    def exposure_set_table(self):
        """Sample tExposureSet PyArrow table."""
        return pa.table({
            "ExposureSetSID": [10, 20, 30],
            "ExposureSetName": ["Set A", "Set B", "Set C"],
            "CompanySID": [1, 1, 1],
        })
    
    @pytest.fixture
    def exposure_view_table(self):
        """Sample tExposureView PyArrow table."""
        return pa.table({
            "ExposureViewSID": [100, 200],
            "ExposureViewName": ["View 1", "View 2"],
            "ExposureSetSID": [10, 20],
        })
    
    def test_extracts_set_metadata(self, exposure_set_table):
        """Should extract exposure set names and count."""
        tables_data = {"tExposureSet": exposure_set_table}
        
        metadata = extract_exposure_metadata(
            tables_data=tables_data,
            tenant_id="test-tenant",
            run_id="test-run",
            source_artifact="test.bak",
        )
        
        assert metadata.exposure_set_count == 3
        assert metadata.exposure_set_names == ["Set A", "Set B", "Set C"]
        assert metadata.tenant_id == "test-tenant"
    
    def test_extracts_view_metadata(self, exposure_set_table, exposure_view_table):
        """Should extract both set and view metadata."""
        tables_data = {
            "tExposureSet": exposure_set_table,
            "tExposureView": exposure_view_table,
        }
        
        metadata = extract_exposure_metadata(
            tables_data=tables_data,
            tenant_id="test-tenant",
            run_id="test-run",
            source_artifact="test.bak",
        )
        
        assert metadata.exposure_set_count == 3
        assert metadata.exposure_view_count == 2
        assert metadata.exposure_view_names == ["View 1", "View 2"]
    
    def test_handles_missing_tables(self):
        """Should handle missing exposure tables gracefully."""
        tables_data = {}  # No exposure tables
        
        metadata = extract_exposure_metadata(
            tables_data=tables_data,
            tenant_id="test-tenant",
            run_id="test-run",
            source_artifact="test.bak",
        )
        
        assert metadata.exposure_set_count == 0
        assert metadata.exposure_view_count == 0
        assert metadata.exposure_set_names == []
    
    def test_handles_null_names(self):
        """Should filter out null values in name columns."""
        table = pa.table({
            "ExposureSetSID": [10, 20, 30],
            "ExposureSetName": ["Set A", None, "Set C"],  # One null
            "CompanySID": [1, 1, 1],
        })
        tables_data = {"tExposureSet": table}
        
        metadata = extract_exposure_metadata(
            tables_data=tables_data,
            tenant_id="test",
            run_id="run",
            source_artifact="test.bak",
        )
        
        assert metadata.exposure_set_count == 3  # Count all rows
        assert metadata.exposure_set_names == ["Set A", "Set C"]  # Filter nulls


class TestCreateExposureClient:
    """Tests for create_exposure_client factory function."""
    
    def test_creates_mock_client(self):
        """Factory should create MockExposureClient when use_mock=True."""
        client = create_exposure_client(use_mock=True)
        
        assert isinstance(client, MockExposureClient)
    
    def test_creates_mock_with_custom_starting_sid(self):
        """Mock client should use custom starting SID."""
        client = create_exposure_client(use_mock=True, mock_starting_sid=5_000_000)
        
        metadata = ExposureMetadata(
            exposure_set_names=[],
            exposure_set_count=1,
            exposure_view_names=[],
            exposure_view_count=0,
            tenant_id="t",
            run_id="r",
            source_artifact="x.bak",
        )
        response = client.create_exposure(metadata)
        
        assert response.exposure_set_sid == 5_000_000
    
    def test_creates_production_client_with_url(self):
        """Factory should create ExposureClient when use_mock=False."""
        client = create_exposure_client(
            api_url="https://exposure-api.example.com",
            use_mock=False,
            tenant_context_lambda_name="test-lambda",
            app_token_secret_name="test-secret",
        )

        assert isinstance(client, ExposureClient)
    
    def test_raises_error_if_missing_url(self):
        """Should raise ValueError if api_url missing for production client."""
        with pytest.raises(ValueError, match="api_url is required"):
            create_exposure_client(use_mock=False)


class TestSIDTransformerOverrideSID:
    """Tests for SID transformer override SID functionality."""
    
    @pytest.fixture
    def transformer(self):
        """Create transformer with mock SID client."""
        return create_transformer(use_mock=True, starting_sid=1_000_000)
    
    @pytest.fixture
    def exposure_set_table(self):
        """Sample tExposureSet PyArrow table."""
        return pa.table({
            "ExposureSetSID": [10, 20, 30],
            "ExposureSetName": ["Set A", "Set B", "Set C"],
            "CompanySID": [1, 2, 3],
        })
    
    @pytest.fixture
    def contract_table(self):
        """Sample tContract table referencing ExposureSetSID."""
        return pa.table({
            "ContractSID": [100, 200, 300],
            "ContractName": ["C1", "C2", "C3"],
            "ExposureSetSID": [10, 20, 30],  # FKs to tExposureSet
        })
    
    def test_set_override_sid_replaces_all_values(self, transformer, exposure_set_table):
        """With override SID, all PKs should map to single value."""
        # Set override - all old ExposureSetSIDs should map to 9999
        override_sid = 9999
        transformer.set_override_sid("tExposureSet", override_sid)
        
        # Transform the table
        result = transformer.transform_table("tExposureSet", exposure_set_table)
        
        # All rows should have the same PK now
        transformed = transformer.get_transformed_table("tExposureSet")
        new_pks = transformed.column("ExposureSetSID").to_pylist()
        
        assert all(pk == override_sid for pk in new_pks)
        assert result.sid_range_start == override_sid
        assert result.sid_range_end == override_sid
    
    def test_override_propagates_to_fk_columns(self, transformer, exposure_set_table, contract_table):
        """FK columns should also use the override SID mapping."""
        override_sid = 9999
        transformer.set_override_sid("tExposureSet", override_sid)
        
        # First transform tExposureSet to establish mapping
        transformer.transform_table("tExposureSet", exposure_set_table)
        
        # Now transform tContract which has FK to tExposureSet
        result = transformer.transform_table("tContract", contract_table)
        
        # Get transformed contract table
        transformed = transformer.get_transformed_table("tContract")
        
        # All FK values should point to the override SID
        fk_values = transformed.column("ExposureSetSID").to_pylist()
        assert all(fk == override_sid for fk in fk_values)
    
    def test_reset_clears_override_sids(self, transformer):
        """reset() should clear override SIDs."""
        transformer.set_override_sid("tExposureSet", 9999)
        
        transformer.reset()
        
        # Override should be cleared - transformation would use allocated SIDs
        assert "tExposureSet" not in transformer._override_sids
    
    def test_without_override_allocates_unique_sids(self, transformer, exposure_set_table):
        """Without override, each row gets unique SID."""
        # No override set
        result = transformer.transform_table("tExposureSet", exposure_set_table)
        
        transformed = transformer.get_transformed_table("tExposureSet")
        new_pks = transformed.column("ExposureSetSID").to_pylist()
        
        # Should have 3 unique SIDs
        assert len(set(new_pks)) == 3
        # Each should be within the allocated range
        assert result.sid_range_start != result.sid_range_end


class TestExposureAPIIntegration:
    """Integration tests for Exposure API → SID Transformer flow."""
    
    def test_full_flow_mock(self):
        """Test complete flow with mock clients."""
        # 1. Create mock exposure client
        exposure_client = MockExposureClient(starting_sid=9_000_000)
        
        # 2. Create mock SID transformer
        transformer = create_transformer(use_mock=True, starting_sid=1_000_000)
        
        # 3. Sample data
        exposure_set_table = pa.table({
            "ExposureSetSID": [10, 20, 30],
            "ExposureSetName": ["A", "B", "C"],
            "CompanySID": [1, 1, 1],
        })
        
        contract_table = pa.table({
            "ContractSID": [100, 200, 300],
            "ExposureSetSID": [10, 20, 30],
        })
        
        # 4. Extract metadata and call Exposure API
        tables_data = {"tExposureSet": exposure_set_table}
        metadata = extract_exposure_metadata(
            tables_data=tables_data,
            tenant_id="test",
            run_id="run-123",
            source_artifact="test.bak",
        )
        
        exposure_response = exposure_client.create_exposure(metadata)
        
        # 5. Set override SID on transformer
        transformer.set_override_sid("tExposureSet", exposure_response.exposure_set_sid)
        
        # 6. Transform tables
        transformer.transform_table("tExposureSet", exposure_set_table)
        transformer.transform_table("tContract", contract_table)
        
        # 7. Verify results
        transformed_sets = transformer.get_transformed_table("tExposureSet")
        transformed_contracts = transformer.get_transformed_table("tContract")
        
        # All PKs should be the override SID
        set_pks = transformed_sets.column("ExposureSetSID").to_pylist()
        assert all(pk == 9_000_000 for pk in set_pks)
        
        # All FK references should point to override SID
        contract_fks = transformed_contracts.column("ExposureSetSID").to_pylist()
        assert all(fk == 9_000_000 for fk in contract_fks)
