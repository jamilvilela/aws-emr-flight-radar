import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.src.dependencies.reader import read_table, get_partition_column


class TestReader:
    """Tests for reader.py"""

    @pytest.fixture
    def sample_df(self, spark):
        """Create sample DataFrame for testing."""
        data = [
            ("A1", "2024-01-01", "U", "2024-01-01 10:00:00"),
            ("A2", "2024-01-01", "I", "2024-01-01 11:00:00"),
            ("A3", "2024-01-02", "D", "2024-01-02 12:00:00"),
        ]
        return spark.createDataFrame(data, ["id", "event_date", "Op", "dms_timestamp"])

    def test_get_partition_column_dims(self):
        """Test partition column for dimension tables."""
        assert get_partition_column("dim_aircraft") == "event_date"
        assert get_partition_column("dim_airport") == "event_date"
        assert get_partition_column("dim_airline") == "event_date"

    def test_get_partition_column_facts(self):
        """Test partition column for fact tables."""
        assert get_partition_column("fact_flight") == "event_date"
        assert get_partition_column("fact_flight_position") == "recorded_at"

    def test_rename_cdc_columns(self, spark, sample_df):
        """Test CDC column renaming."""
        # This test would need a mock table, so we test the logic directly
        from app.src.dependencies.reader import _rename_cdc_columns
        
        df = _rename_cdc_columns(sample_df)
        assert "cdc_operation" in df.columns
        assert "cdc_timestamp" in df.columns
        assert "Op" not in df.columns
        assert "dms_timestamp" not in df.columns


class TestReadTable:
    """Integration-style tests for read_table (require mock catalog)."""

    # These would require a test Glue catalog or mock
    # For now, we test the logic in isolation
    pass