"""
Integration tests for full ingestion pipeline.

These tests require:
- Running SQL Server instance
- Valid AWS credentials with Glue Catalog access
- Test Iceberg tables
"""

import os
import pytest
from src.core.config import AppConfig


# Skip all tests if SQL Server not available
pytestmark = pytest.mark.skipif(
    os.environ.get("INTEGRATION_TESTS") != "1",
    reason="Integration tests disabled. Set INTEGRATION_TESTS=1 to enable."
)


class TestFullIngestion:
    """Integration tests for full pipeline."""
    
    @pytest.fixture
    def config(self):
        """Load configuration from environment."""
        return AppConfig.from_env()
    
    def test_single_table_ingestion(self, config):
        """Test extracting a single table."""
        # TODO: Implement with actual test table
        pass
    
    def test_multiple_table_ingestion(self, config):
        """Test extracting multiple tables in parallel."""
        # TODO: Implement with actual test tables
        pass
    
    def test_with_row_filter(self, config):
        """Test extraction with exposure_id filter."""
        # TODO: Implement with predicate pushdown
        pass
