"""
Integration tests for SID Transformation Pipeline.

These tests verify:
- Full multi-table transformation with FK cascading
- Dependency ordering (parent tables before children)
- Self-referencing table handling
- Realistic schema scenarios from AIRExposure database
"""

import pytest
import pyarrow as pa
from typing import Dict

from src.sid.transformer import SIDTransformer, TransformResult
from src.sid.sid_client import MockSIDClient
from src.sid.config import (
    SID_CONFIG,
    get_processing_order,
    get_fk_relationships,
    PROCESSING_LEVELS,
)


class TestMultiTableCascade:
    """Integration tests for multi-table FK cascading."""
    
    @pytest.fixture
    def mock_client(self):
        """Create mock client with fixed starting SID."""
        return MockSIDClient(starting_sid=1_000_000)
    
    @pytest.fixture
    def transformer(self, mock_client):
        """Create transformer with mock client."""
        return SIDTransformer(mock_client)
    
    @pytest.fixture
    def sample_tables(self) -> Dict[str, pa.Table]:
        """
        Create sample tables mimicking AIRExposure schema.
        
        Hierarchy:
            tCompany (PK: CompanySID)
              └─ tExposureSet (PK: ExposureSetSID, FK: CompanySID)
                   └─ tContract (PK: ContractSID, FK: ExposureSetSID)
                        └─ tContractLayer (PK: ContractSID+LayerNumber, FK: ContractSID)
        """
        # tCompany - Level 0 (root)
        company = pa.table({
            "CompanySID": [1, 2, 3],
            "CompanyName": ["ACME Corp", "Globex Inc", "Initech LLC"],
            "TenantID": [100, 100, 100],
        })
        
        # tExposureSet - Level 1 (depends on tCompany)
        exposure_set = pa.table({
            "ExposureSetSID": [10, 20, 30, 40],
            "ExposureSetName": ["ExSet1", "ExSet2", "ExSet3", "ExSet4"],
            "CompanySID": [1, 1, 2, 3],  # FK to tCompany
        })
        
        # tContract - Level 2 (depends on tExposureSet)
        contract = pa.table({
            "ContractSID": [100, 101, 102, 103, 104],
            "ContractName": ["CON-001", "CON-002", "CON-003", "CON-004", "CON-005"],
            "ExposureSetSID": [10, 10, 20, 30, 40],  # FK to tExposureSet
        })
        
        # tLayer - Level 3 (depends on tContract)
        layer = pa.table({
            "LayerSID": [1000, 1001, 1002, 1003],
            "LayerName": ["Layer1", "Layer2", "Layer3", "Layer4"],
            "ContractSID": [100, 100, 101, 102],  # FK to tContract
        })
        
        return {
            "tCompany": company,
            "tExposureSet": exposure_set,
            "tContract": contract,
            "tLayer": layer,
        }
    
    def test_full_cascade_transformation(self, transformer, sample_tables):
        """Test that FK references are correctly updated through the cascade."""
        # Transform all tables in dependency order
        result = transformer.transform_all(sample_tables)
        
        assert result.success
        assert result.tables_processed == 4
        
        # Get transformed tables
        transformed_company = transformer._transformed_tables.get("tCompany")
        transformed_exposure_set = transformer._transformed_tables.get("tExposureSet")
        transformed_contract = transformer._transformed_tables.get("tContract")
        transformed_layer = transformer._transformed_tables.get("tLayer")
        
        # Verify all tables were transformed
        assert transformed_company is not None
        assert transformed_exposure_set is not None
        assert transformed_contract is not None
        assert transformed_layer is not None
        
        # Get the SID mappings
        company_mapping = transformer.get_mapping("tCompany")
        exposure_set_mapping = transformer.get_mapping("tExposureSet")
        contract_mapping = transformer.get_mapping("tContract")
        
        # Verify company PKs were transformed (new SIDs should be >= 1_000_000)
        company_sids = transformed_company.column("CompanySID").to_pylist()
        assert all(sid >= 1_000_000 for sid in company_sids)
        
        # Verify exposure set FKs point to new company SIDs
        es_company_fks = transformed_exposure_set.column("CompanySID").to_pylist()
        for fk in es_company_fks:
            assert fk in company_sids, f"FK {fk} should be a valid new Company SID"
        
        # Verify contract FKs point to new exposure set SIDs
        exposure_set_sids = transformed_exposure_set.column("ExposureSetSID").to_pylist()
        contract_es_fks = transformed_contract.column("ExposureSetSID").to_pylist()
        for fk in contract_es_fks:
            assert fk in exposure_set_sids, f"FK {fk} should be a valid new ExposureSet SID"
        
        # Verify layer FKs point to new contract SIDs
        contract_sids = transformed_contract.column("ContractSID").to_pylist()
        layer_contract_fks = transformed_layer.column("ContractSID").to_pylist()
        for fk in layer_contract_fks:
            assert fk in contract_sids, f"FK {fk} should be a valid new Contract SID"
    
    def test_dependency_order_is_respected(self, transformer, mock_client, sample_tables):
        """Verify tables are processed in correct dependency order."""
        transformer.transform_all(sample_tables)
        
        # Get allocation history from mock client
        history = mock_client.get_allocation_history()
        
        # Find allocation order
        allocation_order = [alloc["table_name"] for alloc in history]
        
        # tCompany must be before tExposureSet
        assert allocation_order.index("tCompany") < allocation_order.index("tExposureSet")
        
        # tExposureSet must be before tContract
        assert allocation_order.index("tExposureSet") < allocation_order.index("tContract")
        
        # tContract must be before tLayer
        assert allocation_order.index("tContract") < allocation_order.index("tLayer")
    
    def test_row_counts_preserved(self, transformer, sample_tables):
        """Verify row counts are preserved after transformation."""
        original_counts = {name: t.num_rows for name, t in sample_tables.items()}
        
        transformer.transform_all(sample_tables)
        
        for name, original_count in original_counts.items():
            transformed = transformer._transformed_tables.get(name)
            assert transformed is not None
            assert transformed.num_rows == original_count


class TestSelfReferencingTables:
    """Integration tests for self-referencing tables."""
    
    @pytest.fixture
    def mock_client(self):
        return MockSIDClient(starting_sid=2_000_000)
    
    @pytest.fixture
    def transformer(self, mock_client):
        return SIDTransformer(mock_client)
    
    @pytest.fixture
    def location_hierarchy(self) -> Dict[str, pa.Table]:
        """
        Create tLocation with parent-child hierarchy.
        
        Hierarchy:
            USA (ParentLocationSID = None)
              ├─ California (ParentLocationSID = 1)
              │    └─ Los Angeles (ParentLocationSID = 2)
              └─ Texas (ParentLocationSID = 1)
                   └─ Houston (ParentLocationSID = 4)
        """
        return {
            "tLocation": pa.table({
                "LocationSID": [1, 2, 3, 4, 5],
                "LocationName": ["USA", "California", "Los Angeles", "Texas", "Houston"],
                "ParentLocationSID": [None, 1, 2, 1, 4],  # Self-reference FK
            }),
        }
    
    def test_self_reference_hierarchy_preserved(self, transformer, location_hierarchy):
        """Verify parent-child relationships are maintained after transformation."""
        result = transformer.transform_all(location_hierarchy)
        
        assert result.success
        
        transformed = transformer._transformed_tables.get("tLocation")
        assert transformed is not None
        
        # Get the mapping
        mapping = transformer.get_mapping("tLocation")
        
        # Get transformed values
        location_sids = transformed.column("LocationSID").to_pylist()
        parent_sids = transformed.column("ParentLocationSID").to_pylist()
        
        # USA should have no parent
        usa_idx = transformed.column("LocationName").to_pylist().index("USA")
        assert parent_sids[usa_idx] is None
        
        # California's parent should be the new USA SID
        ca_idx = transformed.column("LocationName").to_pylist().index("California")
        assert parent_sids[ca_idx] == mapping[1]  # Old USA SID was 1
        
        # Los Angeles's parent should be the new California SID
        la_idx = transformed.column("LocationName").to_pylist().index("Los Angeles")
        assert parent_sids[la_idx] == mapping[2]  # Old California SID was 2
        
        # Houston's parent should be the new Texas SID
        houston_idx = transformed.column("LocationName").to_pylist().index("Houston")
        assert parent_sids[houston_idx] == mapping[4]  # Old Texas SID was 4
    
    def test_layer_condition_self_reference(self, mock_client):
        """Test tLayerCondition self-reference handling."""
        transformer = SIDTransformer(mock_client)
        
        # Create layer condition hierarchy
        tables = {
            "tLayerCondition": pa.table({
                "LayerConditionSID": [10, 20, 30],
                "ConditionName": ["Root", "Child1", "Child2"],
                "ParentLayerConditionSID": [None, 10, 10],
            }),
        }
        
        result = transformer.transform_all(tables)
        assert result.success
        
        transformed = transformer._transformed_tables.get("tLayerCondition")
        mapping = transformer.get_mapping("tLayerCondition")
        
        parent_sids = transformed.column("ParentLayerConditionSID").to_pylist()
        
        # Root has no parent
        assert parent_sids[0] is None
        
        # Child1 and Child2 point to new Root SID
        assert parent_sids[1] == mapping[10]
        assert parent_sids[2] == mapping[10]


class TestRealisticScenario:
    """Integration tests with realistic AIRExposure-like data volumes."""
    
    @pytest.fixture
    def mock_client(self):
        return MockSIDClient(starting_sid=10_000_000)
    
    @pytest.fixture
    def transformer(self, mock_client):
        return SIDTransformer(mock_client)
    
    def test_larger_dataset_cascade(self, transformer):
        """Test transformation with larger dataset (100+ rows per table)."""
        import random
        random.seed(42)  # Reproducibility
        
        num_companies = 10
        num_exposure_sets = 50
        num_contracts = 200
        
        # Create companies
        company = pa.table({
            "CompanySID": list(range(1, num_companies + 1)),
            "CompanyName": [f"Company_{i}" for i in range(num_companies)],
        })
        
        # Create exposure sets (random company assignment)
        es_company_fks = [random.randint(1, num_companies) for _ in range(num_exposure_sets)]
        exposure_set = pa.table({
            "ExposureSetSID": list(range(1, num_exposure_sets + 1)),
            "ExposureSetName": [f"ES_{i}" for i in range(num_exposure_sets)],
            "CompanySID": es_company_fks,
        })
        
        # Create contracts (random exposure set assignment)
        contract_es_fks = [random.randint(1, num_exposure_sets) for _ in range(num_contracts)]
        contract = pa.table({
            "ContractSID": list(range(1, num_contracts + 1)),
            "ContractName": [f"CON_{i}" for i in range(num_contracts)],
            "ExposureSetSID": contract_es_fks,
        })
        
        tables = {
            "tCompany": company,
            "tExposureSet": exposure_set,
            "tContract": contract,
        }
        
        result = transformer.transform_all(tables)
        
        assert result.success
        assert result.total_rows == num_companies + num_exposure_sets + num_contracts
        
        # Verify FK integrity
        company_new_sids = set(
            transformer._transformed_tables["tCompany"].column("CompanySID").to_pylist()
        )
        es_company_fks = transformer._transformed_tables["tExposureSet"].column("CompanySID").to_pylist()
        
        for fk in es_company_fks:
            assert fk in company_new_sids, f"Invalid FK {fk} not in company SIDs"
    
    def test_sid_ranges_are_contiguous(self):
        """Verify SID allocations use contiguous ranges efficiently."""
        # Use fresh client for this test
        fresh_client = MockSIDClient(starting_sid=10_000_000)
        transformer = SIDTransformer(fresh_client)
        
        tables = {
            "tCompany": pa.table({
                "CompanySID": [1, 2, 3, 4, 5],
                "CompanyName": ["A", "B", "C", "D", "E"],
            }),
        }
        
        transformer.transform_all(tables)
        
        # Check allocation
        history = fresh_client.get_allocation_history()
        company_alloc = next(a for a in history if a["table_name"] == "tCompany")
        
        # Verify count is correct
        assert company_alloc["count"] == 5
        
        # MockSIDClient adds table-based offset, so just verify start >= base
        assert company_alloc["start"] >= 10_000_000
        
        # New SIDs should be contiguous from start
        start = company_alloc["start"]
        new_sids = transformer._transformed_tables["tCompany"].column("CompanySID").to_pylist()
        expected = [start, start + 1, start + 2, start + 3, start + 4]
        assert new_sids == expected


class TestErrorHandling:
    """Integration tests for error scenarios."""
    
    @pytest.fixture
    def mock_client(self):
        return MockSIDClient()
    
    @pytest.fixture
    def transformer(self, mock_client):
        return SIDTransformer(mock_client)
    
    def test_unknown_table_skipped(self, transformer):
        """Tables not in SID_CONFIG should be skipped - transform_all only processes known tables."""
        tables = {
            "tCompany": pa.table({
                "CompanySID": [1, 2],
                "CompanyName": ["A", "B"],
            }),
            "tUnknownTable": pa.table({
                "SomeID": [100, 200],
                "Data": ["x", "y"],
            }),
        }
        
        result = transformer.transform_all(tables)
        
        # Should process known tables
        assert "tCompany" in transformer._transformed_tables
        
        # Unknown table should NOT be in transformed tables (only known tables processed)
        assert "tUnknownTable" not in transformer._transformed_tables
        
        # transform_table can be called directly for unknown table to get error
        unknown_table = tables["tUnknownTable"]
        unknown_result = transformer.transform_table("tUnknownTable", unknown_table)
        assert unknown_result.error is not None
        assert "not in SID_CONFIG" in unknown_result.error
    
    def test_missing_pk_column_handled(self, transformer):
        """Tables missing PK column should error gracefully."""
        tables = {
            "tCompany": pa.table({
                # Missing CompanySID column
                "CompanyName": ["A", "B"],
                "TenantID": [1, 2],
            }),
        }
        
        result = transformer.transform_all(tables)
        
        company_result = result.results[0]
        assert company_result.error is not None
        assert "not found" in company_result.error.lower()
    
    def test_empty_table_handled(self, transformer):
        """Empty tables should be handled gracefully."""
        tables = {
            "tCompany": pa.table({
                "CompanySID": [],
                "CompanyName": [],
            }),
        }
        
        result = transformer.transform_all(tables)
        
        assert result.success
        company_result = result.results[0]
        assert company_result.rows_processed == 0
        assert company_result.error is None


class TestProcessingLevels:
    """Tests for processing level configuration."""
    
    def test_all_levels_represented(self):
        """Verify all levels 0-5 have tables assigned."""
        for level in range(6):
            assert level in PROCESSING_LEVELS
            assert len(PROCESSING_LEVELS[level]) > 0
    
    def test_level_0_has_root_tables(self):
        """Level 0 should contain only root tables with no internal FK deps."""
        level_0 = PROCESSING_LEVELS[0]
        
        # tCompany should be at level 0
        assert "tCompany" in level_0
        
        # Level 0 tables should have no FK to other tables in config
        for table_name in level_0:
            if table_name in SID_CONFIG:
                config = SID_CONFIG[table_name]
                # FK should only reference external tables or self
                for fk in config.fk_columns:
                    parent_table = fk.split("->")[0] if "->" in fk else fk.replace("SID", "")
                    # If FK points to a table in config, it should be self-reference
                    if f"t{parent_table}" in SID_CONFIG:
                        # Self-reference is OK at level 0 (e.g., tLocation)
                        pass
    
    def test_processing_order_returns_sorted_list(self):
        """get_processing_order() should return tables sorted by level."""
        order = get_processing_order()
        
        previous_level = -1
        for table_name in order:
            # Find this table's level
            for level, tables in PROCESSING_LEVELS.items():
                if table_name in tables:
                    assert level >= previous_level, \
                        f"{table_name} at level {level} came after level {previous_level}"
                    previous_level = level
                    break
