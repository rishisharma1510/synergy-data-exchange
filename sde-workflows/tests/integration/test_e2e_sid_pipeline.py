"""
End-to-End Tests for SID Transformation Pipeline.

Tests the complete flow: Read → Transform SIDs → Write
Uses mocked components to simulate full ingestion with SID transformation.
"""

import pytest
import pyarrow as pa
from dataclasses import dataclass
from typing import Dict, List, Optional
from unittest.mock import Mock, MagicMock, patch

from src.sid.transformer import SIDTransformer, TransformResult, TransformJobResult
from src.sid.sid_client import MockSIDClient
from src.sid.config import SID_CONFIG, get_processing_order


# ============================================================================
# Mock Components
# ============================================================================

class MockSqlServerReader:
    """Mock SQL Server reader that returns predefined Arrow tables."""
    
    def __init__(self, tables: Dict[str, pa.Table]):
        """
        Initialize mock reader.
        
        Args:
            tables: Dict mapping table_name -> Arrow Table.
        """
        self._tables = tables
        self.read_calls: List[str] = []
    
    def read_table(
        self, 
        table_name: str, 
        schema: str = "dbo",
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
    ) -> pa.Table:
        """Return mocked table data."""
        self.read_calls.append(table_name)
        
        if table_name not in self._tables:
            # Return empty table with minimal schema
            return pa.table({})
        
        table = self._tables[table_name]
        
        # Apply column filter if specified
        if columns:
            available_cols = [c for c in columns if c in table.column_names]
            table = table.select(available_cols)
        
        return table
    
    def get_table_names(self) -> List[str]:
        """Return list of available tables."""
        return list(self._tables.keys())


class MockIcebergWriter:
    """Mock Iceberg writer that captures written data."""
    
    def __init__(self):
        self.written_tables: Dict[str, List[pa.Table]] = {}
        self.created_tables: List[str] = []
        self.append_calls: List[Dict] = []
    
    def create_table_if_not_exists(
        self, 
        namespace: str, 
        table_name: str, 
        schema: pa.Schema,
        location: Optional[str] = None,
    ):
        """Mock table creation."""
        full_name = f"{namespace}.{table_name}"
        self.created_tables.append(full_name)
        return Mock()
    
    def append(
        self, 
        namespace: str, 
        table_name: str, 
        data: pa.Table,
    ) -> int:
        """Mock append - captures written data."""
        full_name = f"{namespace}.{table_name}"
        
        if full_name not in self.written_tables:
            self.written_tables[full_name] = []
        
        self.written_tables[full_name].append(data)
        
        self.append_calls.append({
            "namespace": namespace,
            "table_name": table_name,
            "row_count": data.num_rows,
        })
        
        return data.num_rows
    
    def get_written_data(self, namespace: str, table_name: str) -> Optional[pa.Table]:
        """Get combined written data for a table."""
        full_name = f"{namespace}.{table_name}"
        tables = self.written_tables.get(full_name, [])
        
        if not tables:
            return None
        
        return pa.concat_tables(tables)


# ============================================================================
# Pipeline Simulation
# ============================================================================

def simulate_ingestion_pipeline(
    reader: MockSqlServerReader,
    writer: MockIcebergWriter,
    transformer: SIDTransformer,
    tables_to_process: List[str],
    namespace: str = "test_db",
    sid_enabled: bool = True,
) -> Dict[str, any]:
    """
    Simulate the full ingestion pipeline.
    
    Flow:
        1. Read tables from SQL Server (mocked)
        2. If SID enabled, transform all tables in dependency order
        3. Write to Iceberg (mocked)
    
    Returns:
        Dict with processing results and statistics.
    """
    results = {
        "tables_read": [],
        "tables_transformed": [],
        "tables_written": [],
        "total_rows_read": 0,
        "total_rows_written": 0,
        "sid_enabled": sid_enabled,
        "transform_result": None,
    }
    
    # Step 1: Read all tables
    read_tables = {}
    for table_name in tables_to_process:
        arrow_table = reader.read_table(table_name)
        if arrow_table.num_rows > 0:
            read_tables[table_name] = arrow_table
            results["tables_read"].append(table_name)
            results["total_rows_read"] += arrow_table.num_rows
    
    # Step 2: Transform SIDs if enabled
    if sid_enabled and read_tables:
        transform_result = transformer.transform_all(read_tables)
        results["transform_result"] = transform_result
        results["tables_transformed"] = [r.table_name for r in transform_result.results]
        
        # Use transformed tables
        tables_to_write = transformer._transformed_tables
    else:
        tables_to_write = read_tables
    
    # Step 3: Write to Iceberg
    for table_name, arrow_table in tables_to_write.items():
        # Create table if needed
        writer.create_table_if_not_exists(
            namespace, 
            table_name, 
            arrow_table.schema,
        )
        
        # Append data
        rows = writer.append(namespace, table_name, arrow_table)
        results["tables_written"].append(table_name)
        results["total_rows_written"] += rows
    
    return results


# ============================================================================
# Test Cases
# ============================================================================

class TestEndToEndPipeline:
    """End-to-end tests for complete ingestion flow."""
    
    @pytest.fixture
    def sample_data(self) -> Dict[str, pa.Table]:
        """Create realistic sample data matching AIRExposure schema."""
        return {
            "tCompany": pa.table({
                "CompanySID": [1, 2, 3],
                "CompanyName": ["Alpha Corp", "Beta Inc", "Gamma LLC"],
                "TenantID": [100, 100, 100],
            }),
            "tExposureSet": pa.table({
                "ExposureSetSID": [10, 20, 30, 40],
                "ExposureSetName": ["Portfolio A", "Portfolio B", "Portfolio C", "Portfolio D"],
                "CompanySID": [1, 1, 2, 3],
            }),
            "tContract": pa.table({
                "ContractSID": [100, 101, 102, 103],
                "ContractName": ["CON-001", "CON-002", "CON-003", "CON-004"],
                "ExposureSetSID": [10, 10, 20, 30],
            }),
            "tLayer": pa.table({
                "LayerSID": [1000, 1001, 1002],
                "LayerName": ["Primary", "XS1", "XS2"],
                "ContractSID": [100, 100, 101],
            }),
        }
    
    @pytest.fixture
    def mock_client(self):
        return MockSIDClient(starting_sid=5_000_000)
    
    @pytest.fixture
    def transformer(self, mock_client):
        return SIDTransformer(mock_client)
    
    def test_full_pipeline_with_sid_transformation(self, sample_data, transformer):
        """Test complete pipeline: read → transform → write."""
        reader = MockSqlServerReader(sample_data)
        writer = MockIcebergWriter()
        
        results = simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=list(sample_data.keys()),
            sid_enabled=True,
        )
        
        # Verify all tables processed
        assert len(results["tables_read"]) == 4
        assert len(results["tables_written"]) == 4
        assert results["total_rows_read"] == 14  # 3+4+4+3
        assert results["total_rows_written"] == 14
        
        # Verify transformation happened
        assert results["transform_result"].success
        
        # Verify FK integrity in written data
        written_company = writer.get_written_data("test_db", "tCompany")
        written_es = writer.get_written_data("test_db", "tExposureSet")
        
        company_sids = set(written_company.column("CompanySID").to_pylist())
        es_company_fks = set(written_es.column("CompanySID").to_pylist())
        
        # All FK values should reference valid PK values
        assert es_company_fks.issubset(company_sids)
    
    def test_pipeline_without_sid_transformation(self, sample_data):
        """Test pipeline with SID transformation disabled."""
        reader = MockSqlServerReader(sample_data)
        writer = MockIcebergWriter()
        transformer = SIDTransformer(MockSIDClient())
        
        results = simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=list(sample_data.keys()),
            sid_enabled=False,  # Disabled
        )
        
        # Tables should be written without transformation
        assert results["transform_result"] is None
        assert len(results["tables_written"]) == 4
        
        # Original SIDs should be preserved
        written_company = writer.get_written_data("test_db", "tCompany")
        sids = written_company.column("CompanySID").to_pylist()
        assert sids == [1, 2, 3]  # Original values
    
    def test_pipeline_preserves_data_columns(self, sample_data, transformer):
        """Verify non-SID columns are preserved through transformation."""
        reader = MockSqlServerReader(sample_data)
        writer = MockIcebergWriter()
        
        simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=list(sample_data.keys()),
            sid_enabled=True,
        )
        
        # Check company names are preserved
        written_company = writer.get_written_data("test_db", "tCompany")
        names = written_company.column("CompanyName").to_pylist()
        assert names == ["Alpha Corp", "Beta Inc", "Gamma LLC"]
        
        # Check contract names are preserved
        written_contract = writer.get_written_data("test_db", "tContract")
        contract_names = written_contract.column("ContractName").to_pylist()
        assert contract_names == ["CON-001", "CON-002", "CON-003", "CON-004"]
    
    def test_pipeline_handles_empty_tables(self, transformer):
        """Test pipeline with some empty tables."""
        data = {
            "tCompany": pa.table({
                "CompanySID": [1, 2],
                "CompanyName": ["A", "B"],
            }),
            "tExposureSet": pa.table({
                "ExposureSetSID": [],
                "ExposureSetName": [],
                "CompanySID": [],
            }),
        }
        
        reader = MockSqlServerReader(data)
        writer = MockIcebergWriter()
        
        results = simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=list(data.keys()),
            sid_enabled=True,
        )
        
        # Should process non-empty table
        assert "tCompany" in results["tables_read"]
        assert results["total_rows_written"] >= 2


class TestSIDAllocationTracking:
    """Tests for SID allocation tracking in E2E scenario."""
    
    def test_allocation_count_matches_row_count(self):
        """Verify SID allocations request correct counts."""
        mock_client = MockSIDClient(starting_sid=1_000_000)
        transformer = SIDTransformer(mock_client)
        
        data = {
            "tCompany": pa.table({
                "CompanySID": list(range(1, 101)),  # 100 rows
                "CompanyName": [f"Company_{i}" for i in range(100)],
            }),
        }
        
        reader = MockSqlServerReader(data)
        writer = MockIcebergWriter()
        
        simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=["tCompany"],
            sid_enabled=True,
        )
        
        # Check allocation history
        history = mock_client.get_allocation_history()
        company_alloc = next(a for a in history if a["table_name"] == "tCompany")
        
        assert company_alloc["count"] == 100
    
    def test_multiple_runs_get_unique_sids(self):
        """Verify consecutive runs get different SID ranges."""
        mock_client = MockSIDClient(starting_sid=1_000_000)
        
        # Run 1
        transformer1 = SIDTransformer(mock_client)
        data1 = {"tCompany": pa.table({
            "CompanySID": [1, 2, 3],
            "CompanyName": ["A", "B", "C"],
        })}
        
        result1 = transformer1.transform_all(data1)
        sids_run1 = transformer1._transformed_tables["tCompany"].column("CompanySID").to_pylist()
        
        # Run 2 (same client maintains state)
        transformer2 = SIDTransformer(mock_client)
        data2 = {"tCompany": pa.table({
            "CompanySID": [10, 20, 30],
            "CompanyName": ["X", "Y", "Z"],
        })}
        
        result2 = transformer2.transform_all(data2)
        sids_run2 = transformer2._transformed_tables["tCompany"].column("CompanySID").to_pylist()
        
        # SIDs should not overlap
        assert set(sids_run1).isdisjoint(set(sids_run2))


class TestCascadingFKIntegrity:
    """Tests for FK integrity across multiple table levels."""
    
    def test_four_level_cascade_integrity(self):
        """Test 4-level FK cascade: Company → ExposureSet → Contract → Layer."""
        mock_client = MockSIDClient(starting_sid=10_000_000)
        transformer = SIDTransformer(mock_client)
        
        # Create hierarchical data
        data = {
            "tCompany": pa.table({
                "CompanySID": [1, 2],
                "CompanyName": ["Parent1", "Parent2"],
            }),
            "tExposureSet": pa.table({
                "ExposureSetSID": [10, 20, 30],
                "ExposureSetName": ["ES1", "ES2", "ES3"],
                "CompanySID": [1, 1, 2],  # FK to Company
            }),
            "tContract": pa.table({
                "ContractSID": [100, 101, 102, 103],
                "ContractName": ["C1", "C2", "C3", "C4"],
                "ExposureSetSID": [10, 10, 20, 30],  # FK to ExposureSet
            }),
            "tLayer": pa.table({
                "LayerSID": [1000, 1001, 1002, 1003, 1004],
                "LayerName": ["L1", "L2", "L3", "L4", "L5"],
                "ContractSID": [100, 100, 101, 102, 103],  # FK to Contract
            }),
        }
        
        reader = MockSqlServerReader(data)
        writer = MockIcebergWriter()
        
        results = simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=list(data.keys()),
            sid_enabled=True,
        )
        
        assert results["transform_result"].success
        
        # Verify each FK level
        company_sids = set(
            writer.get_written_data("test_db", "tCompany")
            .column("CompanySID").to_pylist()
        )
        es_company_fks = set(
            writer.get_written_data("test_db", "tExposureSet")
            .column("CompanySID").to_pylist()
        )
        assert es_company_fks.issubset(company_sids), "ExposureSet FKs invalid"
        
        es_sids = set(
            writer.get_written_data("test_db", "tExposureSet")
            .column("ExposureSetSID").to_pylist()
        )
        contract_es_fks = set(
            writer.get_written_data("test_db", "tContract")
            .column("ExposureSetSID").to_pylist()
        )
        assert contract_es_fks.issubset(es_sids), "Contract FKs invalid"
        
        contract_sids = set(
            writer.get_written_data("test_db", "tContract")
            .column("ContractSID").to_pylist()
        )
        layer_contract_fks = set(
            writer.get_written_data("test_db", "tLayer")
            .column("ContractSID").to_pylist()
        )
        assert layer_contract_fks.issubset(contract_sids), "Layer FKs invalid"


class TestSelfReferencingE2E:
    """End-to-end tests for self-referencing tables."""
    
    def test_location_hierarchy_preserved(self):
        """Test tLocation parent-child hierarchy through pipeline."""
        mock_client = MockSIDClient(starting_sid=50_000_000)
        transformer = SIDTransformer(mock_client)
        
        # Hierarchical location data
        data = {
            "tLocation": pa.table({
                "LocationSID": [1, 2, 3, 4, 5],
                "LocationName": ["World", "NA", "USA", "CA", "LA"],
                "ParentLocationSID": [None, 1, 2, 3, 4],
            }),
        }
        
        reader = MockSqlServerReader(data)
        writer = MockIcebergWriter()
        
        simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=["tLocation"],
            sid_enabled=True,
        )
        
        written = writer.get_written_data("test_db", "tLocation")
        mapping = transformer.get_mapping("tLocation")
        
        names = written.column("LocationName").to_pylist()
        sids = written.column("LocationSID").to_pylist()
        parents = written.column("ParentLocationSID").to_pylist()
        
        # World (index 0) should have no parent
        world_idx = names.index("World")
        assert parents[world_idx] is None
        
        # NA's parent should be new World SID
        na_idx = names.index("NA")
        assert parents[na_idx] == mapping[1]  # Old World SID was 1
        
        # USA's parent should be new NA SID
        usa_idx = names.index("USA")
        assert parents[usa_idx] == mapping[2]  # Old NA SID was 2
        
        # CA's parent should be new USA SID
        ca_idx = names.index("CA")
        assert parents[ca_idx] == mapping[3]
        
        # LA's parent should be new CA SID
        la_idx = names.index("LA")
        assert parents[la_idx] == mapping[4]


class TestPerformance:
    """Performance-oriented tests with larger datasets."""
    
    def test_large_dataset_completes_quickly(self):
        """Test with 10K+ rows completes in reasonable time."""
        import time
        
        mock_client = MockSIDClient(starting_sid=100_000_000)
        transformer = SIDTransformer(mock_client)
        
        # Create larger dataset
        num_companies = 100
        num_exposure_sets = 1000
        num_contracts = 5000
        num_layers = 10000
        
        import random
        random.seed(42)
        
        data = {
            "tCompany": pa.table({
                "CompanySID": list(range(1, num_companies + 1)),
                "CompanyName": [f"Company_{i}" for i in range(num_companies)],
            }),
            "tExposureSet": pa.table({
                "ExposureSetSID": list(range(1, num_exposure_sets + 1)),
                "ExposureSetName": [f"ES_{i}" for i in range(num_exposure_sets)],
                "CompanySID": [random.randint(1, num_companies) for _ in range(num_exposure_sets)],
            }),
            "tContract": pa.table({
                "ContractSID": list(range(1, num_contracts + 1)),
                "ContractName": [f"CON_{i}" for i in range(num_contracts)],
                "ExposureSetSID": [random.randint(1, num_exposure_sets) for _ in range(num_contracts)],
            }),
            "tLayer": pa.table({
                "LayerSID": list(range(1, num_layers + 1)),
                "LayerName": [f"Layer_{i}" for i in range(num_layers)],
                "ContractSID": [random.randint(1, num_contracts) for _ in range(num_layers)],
            }),
        }
        
        reader = MockSqlServerReader(data)
        writer = MockIcebergWriter()
        
        start = time.time()
        results = simulate_ingestion_pipeline(
            reader=reader,
            writer=writer,
            transformer=transformer,
            tables_to_process=list(data.keys()),
            sid_enabled=True,
        )
        elapsed = time.time() - start
        
        total_rows = num_companies + num_exposure_sets + num_contracts + num_layers
        
        assert results["transform_result"].success
        assert results["total_rows_written"] == total_rows
        
        # Should complete in under 5 seconds (usually <1s)
        assert elapsed < 5.0, f"Pipeline took too long: {elapsed:.2f}s"
        
        # Verify FK integrity for sample
        company_sids = set(
            writer.get_written_data("test_db", "tCompany")
            .column("CompanySID").to_pylist()
        )
        es_fks = writer.get_written_data("test_db", "tExposureSet").column("CompanySID").to_pylist()
        
        # Check first 100 FKs
        for fk in es_fks[:100]:
            assert fk in company_sids
