"""
Unit tests for SID Transformer module.
"""

import pytest
import pyarrow as pa

from src.sid.config import (
    SID_CONFIG,
    get_processing_order,
    get_fk_relationships,
    get_self_reference_fk,
    get_pk_column,
    get_tables_with_self_reference,
)
from src.sid.sid_client import MockSIDClient, SIDAllocationResponse
from src.sid.transformer import SIDTransformer, create_transformer


class TestMockSIDClient:
    """Tests for MockSIDClient."""
    
    def test_allocate_returns_contiguous_range(self):
        client = MockSIDClient(starting_sid=1000)
        
        response = client.allocate("tCompany", 100)
        
        assert response.count == 100
        assert response.end == response.start + 99
    
    def test_allocate_increments_for_same_table(self):
        client = MockSIDClient(starting_sid=1000)
        
        response1 = client.allocate("tCompany", 100)
        response2 = client.allocate("tCompany", 50)
        
        # Second allocation should continue from where first ended
        assert response2.start == response1.end + 1
    
    def test_allocate_different_tables_have_different_ranges(self):
        client = MockSIDClient()
        
        response1 = client.allocate("tCompany", 100)
        response2 = client.allocate("tLocation", 100)
        
        # Different tables should have non-overlapping ranges
        assert response1.start != response2.start
    
    def test_allocate_rejects_zero_count(self):
        client = MockSIDClient()
        
        with pytest.raises(ValueError):
            client.allocate("tCompany", 0)
    
    def test_allocate_rejects_negative_count(self):
        client = MockSIDClient()
        
        with pytest.raises(ValueError):
            client.allocate("tCompany", -1)
    
    def test_allocation_history_tracking(self):
        client = MockSIDClient()
        
        client.allocate("tCompany", 100)
        client.allocate("tLocation", 50)
        
        history = client.get_allocation_history()
        
        assert len(history) == 2
        assert history[0]["table_name"] == "tCompany"
        assert history[0]["count"] == 100
        assert history[1]["table_name"] == "tLocation"
        assert history[1]["count"] == 50
    
    def test_reset_clears_state(self):
        client = MockSIDClient()
        
        client.allocate("tCompany", 100)
        client.reset()
        
        assert len(client.get_allocation_history()) == 0


class TestSIDConfig:
    """Tests for SID configuration."""
    
    def test_processing_order_starts_with_level_0(self):
        order = get_processing_order()
        
        # First items should be level 0
        assert order[0][0] == 0
    
    def test_processing_order_is_sorted_by_level(self):
        order = get_processing_order()
        
        levels = [level for level, _ in order]
        assert levels == sorted(levels)
    
    def test_tcompany_has_no_internal_fk_dependencies(self):
        fk_rels = get_fk_relationships("tCompany")
        
        # tCompany FK columns reference EXTERNAL tables only
        # get_fk_relationships filters out EXTERNAL
        for parent in fk_rels.values():
            assert parent != "EXTERNAL"
    
    def test_tlocation_has_self_reference(self):
        self_ref = get_self_reference_fk("tLocation")
        
        assert self_ref == "ParentLocationSID"
    
    def test_tlayercondition_has_self_reference(self):
        self_ref = get_self_reference_fk("tLayerCondition")
        
        assert self_ref == "ParentLayerConditionSID"
    
    def test_get_tables_with_self_reference(self):
        tables = get_tables_with_self_reference()
        
        assert "tLocation" in tables
        assert "tLayerCondition" in tables
        assert "tCompanyLossAssociation" in tables
    
    def test_texposureset_depends_on_tcompany(self):
        fk_rels = get_fk_relationships("tExposureSet")
        
        assert "CompanySID" in fk_rels
        assert fk_rels["CompanySID"] == "tCompany"
    
    def test_tcontract_depends_on_texposureset(self):
        fk_rels = get_fk_relationships("tContract")
        
        assert "ExposureSetSID" in fk_rels
        assert fk_rels["ExposureSetSID"] == "tExposureSet"


class TestSIDTransformer:
    """Tests for SIDTransformer."""
    
    @pytest.fixture
    def transformer(self):
        """Create a transformer with mock client."""
        return create_transformer(use_mock=True, starting_sid=1_000_000)
    
    @pytest.fixture
    def sample_company_table(self):
        """Create sample tCompany PyArrow table."""
        return pa.table({
            "CompanySID": [1, 2, 3],
            "CompanyName": ["Company A", "Company B", "Company C"],
            "BusinessUnitSID": [100, 100, 101],
            "DefaultExposureSetSID": [None, None, None],
        })
    
    @pytest.fixture
    def sample_exposureset_table(self):
        """Create sample tExposureSet PyArrow table."""
        return pa.table({
            "ExposureSetSID": [10, 20, 30],
            "ExposureSetName": ["Set A", "Set B", "Set C"],
            "CompanySID": [1, 2, 3],  # References tCompany
        })
    
    @pytest.fixture
    def sample_location_table(self):
        """Create sample tLocation table with self-reference."""
        return pa.table({
            "LocationSID": [100, 101, 102, 103],
            "LocationName": ["Building A", "Floor 1", "Floor 2", "Room 101"],
            "ContractSID": [1, 1, 1, 1],
            "ExposureSetSID": [10, 10, 10, 10],
            "ParentLocationSID": [None, 100, 100, 101],  # Self-reference
            "GeographySID": [1, 1, 1, 1],
        })
    
    def test_transform_table_updates_pk(self, transformer, sample_company_table):
        result = transformer.transform_table("tCompany", sample_company_table)
        
        assert result.rows_processed == 3
        assert result.pk_column == "CompanySID"
        assert result.error is None
        
        # Check mapping was created
        mapping = transformer.get_mapping("tCompany")
        assert len(mapping) == 3
        assert 1 in mapping and 2 in mapping and 3 in mapping
    
    def test_transform_table_creates_new_sids(self, transformer, sample_company_table):
        result = transformer.transform_table("tCompany", sample_company_table)
        
        transformed = transformer.get_transformed_table("tCompany")
        new_sids = transformed.column("CompanySID").to_pylist()
        
        # New SIDs should be >= 1_000_000 (starting_sid)
        for sid in new_sids:
            assert sid >= 1_000_000
    
    def test_transform_table_preserves_other_columns(self, transformer, sample_company_table):
        transformer.transform_table("tCompany", sample_company_table)
        
        transformed = transformer.get_transformed_table("tCompany")
        
        # Other columns should be unchanged
        assert transformed.column("CompanyName").to_pylist() == ["Company A", "Company B", "Company C"]
    
    def test_transform_table_updates_fk_columns(self, transformer, sample_company_table, sample_exposureset_table):
        # First transform parent table
        transformer.transform_table("tCompany", sample_company_table)
        company_mapping = transformer.get_mapping("tCompany")
        
        # Then transform child table
        result = transformer.transform_table("tExposureSet", sample_exposureset_table)
        
        assert "CompanySID" in result.fk_columns_updated
        
        transformed = transformer.get_transformed_table("tExposureSet")
        new_company_sids = transformed.column("CompanySID").to_pylist()
        
        # FK values should be mapped to new parent SIDs
        for old_sid, new_sid in zip([1, 2, 3], new_company_sids):
            assert new_sid == company_mapping[old_sid]
    
    def test_transform_table_handles_self_reference(self, transformer, sample_location_table):
        # Skip FK update to test self-reference separately
        # (In real scenario, would need to transform Contract and ExposureSet first)
        
        # Add tContract and tExposureSet to mappings manually
        transformer._sid_mappings["tContract"] = {1: 5_000_001}
        transformer._sid_mappings["tExposureSet"] = {10: 6_000_001}
        
        result = transformer.transform_table("tLocation", sample_location_table)
        
        assert result.self_reference_updated
        assert "ParentLocationSID" in result.fk_columns_updated
        
        transformed = transformer.get_transformed_table("tLocation")
        parent_sids = transformed.column("ParentLocationSID").to_pylist()
        
        # First row has no parent (None)
        assert parent_sids[0] is None
        
        # Other rows should reference new LocationSIDs
        location_mapping = transformer.get_mapping("tLocation")
        assert parent_sids[1] == location_mapping[100]  # Floor 1 -> Building A
        assert parent_sids[2] == location_mapping[100]  # Floor 2 -> Building A
        assert parent_sids[3] == location_mapping[101]  # Room 101 -> Floor 1
    
    def test_transform_table_handles_empty_table(self, transformer):
        empty_table = pa.table({
            "CompanySID": pa.array([], type=pa.int32()),
            "CompanyName": pa.array([], type=pa.string()),
        })
        
        result = transformer.transform_table("tCompany", empty_table)
        
        assert result.rows_processed == 0
        assert result.error is None
    
    def test_transform_table_handles_unknown_table(self, transformer):
        unknown_table = pa.table({
            "ID": [1, 2, 3],
        })
        
        result = transformer.transform_table("tUnknownTable", unknown_table)
        
        assert result.error is not None
        assert "not in SID_CONFIG" in result.error
    
    def test_transform_table_handles_missing_pk_column(self, transformer):
        bad_table = pa.table({
            "WrongColumn": [1, 2, 3],
        })
        
        result = transformer.transform_table("tCompany", bad_table)
        
        assert result.error is not None
        assert "not found" in result.error
    
    def test_transform_all_processes_in_order(self, transformer, sample_company_table, sample_exposureset_table):
        tables = {
            "tCompany": sample_company_table,
            "tExposureSet": sample_exposureset_table,
        }
        
        processed_order = []
        
        def track_order(result):
            processed_order.append(result.table_name)
        
        result = transformer.transform_all(tables, on_table_complete=track_order)
        
        assert result.success
        # tCompany should be processed before tExposureSet (Level 0 before Level 1)
        assert processed_order.index("tCompany") < processed_order.index("tExposureSet")
    
    def test_transform_all_skips_missing_tables(self, transformer, sample_company_table):
        tables = {
            "tCompany": sample_company_table,
            # tExposureSet is missing
        }
        
        result = transformer.transform_all(tables)
        
        assert result.success
        assert result.tables_processed == 1
    
    def test_reset_clears_state(self, transformer, sample_company_table):
        transformer.transform_table("tCompany", sample_company_table)
        
        assert len(transformer.get_mapping("tCompany")) > 0
        
        transformer.reset()
        
        assert len(transformer.get_mapping("tCompany")) == 0
        assert transformer.get_transformed_table("tCompany") is None


class TestSIDAllocationResponse:
    """Tests for SIDAllocationResponse."""
    
    def test_end_calculation(self):
        response = SIDAllocationResponse(start=1000, count=100)
        
        assert response.end == 1099
    
    def test_end_with_count_1(self):
        response = SIDAllocationResponse(start=500, count=1)
        
        assert response.end == 500


class TestIntegrationScenario:
    """Integration tests with realistic data flow."""
    
    def test_full_parent_child_cascade(self):
        """Test complete parent→child SID transformation flow."""
        transformer = create_transformer(use_mock=True, starting_sid=5_000_000)
        
        # Level 0: tCompany
        company_table = pa.table({
            "CompanySID": [1, 2],
            "CompanyName": ["Acme Corp", "Beta Inc"],
            "BusinessUnitSID": [100, 100],
            "DefaultExposureSetSID": [None, None],
        })
        
        # Level 1: tExposureSet
        exposureset_table = pa.table({
            "ExposureSetSID": [10, 20, 30],
            "CompanySID": [1, 1, 2],
            "ExposureSetName": ["Set 1", "Set 2", "Set 3"],
        })
        
        # Level 2: tContract
        contract_table = pa.table({
            "ContractSID": [100, 200, 300],
            "ExposureSetSID": [10, 20, 30],
            "ContractName": ["Contract A", "Contract B", "Contract C"],
        })
        
        tables = {
            "tCompany": company_table,
            "tExposureSet": exposureset_table,
            "tContract": contract_table,
        }
        
        result = transformer.transform_all(tables)
        
        assert result.success
        assert result.tables_processed == 3
        
        # Verify cascading FK updates
        company_mapping = transformer.get_mapping("tCompany")
        exposureset_mapping = transformer.get_mapping("tExposureSet")
        
        # tExposureSet.CompanySID should point to new tCompany.CompanySID values
        transformed_exposureset = transformer.get_transformed_table("tExposureSet")
        new_company_refs = transformed_exposureset.column("CompanySID").to_pylist()
        
        assert new_company_refs[0] == company_mapping[1]  # Set 1 -> Acme Corp
        assert new_company_refs[1] == company_mapping[1]  # Set 2 -> Acme Corp
        assert new_company_refs[2] == company_mapping[2]  # Set 3 -> Beta Inc
        
        # tContract.ExposureSetSID should point to new tExposureSet.ExposureSetSID values
        transformed_contract = transformer.get_transformed_table("tContract")
        new_exposureset_refs = transformed_contract.column("ExposureSetSID").to_pylist()
        
        assert new_exposureset_refs[0] == exposureset_mapping[10]
        assert new_exposureset_refs[1] == exposureset_mapping[20]
        assert new_exposureset_refs[2] == exposureset_mapping[30]
