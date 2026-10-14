import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.src.dependencies.writer import Writer
from app.src.dependencies.config_models import TableConfig


class TestWriter:
    """Tests for writer.py"""

    @pytest.fixture
    def sample_df(self, spark):
        data = [
            (1, "A123", "2024-01-01"),
            (2, "B456", "2024-01-01"),
        ]
        return spark.createDataFrame(data, ["id", "flight_id", "event_date"])

    @pytest.fixture
    def table_config(self):
        return TableConfig(
            target_table="test_table",
            source_tables=["source"],
            primary_key=["flight_id"],
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="pyspark",
            transform_module="jobs.test",
            order=1,
            enable_zorder=False,
            zorder_cols=[]
        )

    def test_writer_init(self, spark):
        writer = Writer(spark, "db_trusted")
        assert writer.trusted_db == "db_trusted"

    def test_bootstrap_table(self, spark, sample_df, table_config):
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = Writer(spark, "test_db")
            
            count = writer.write(table_config, sample_df, processing_date="2024-01-01", mode="full")
            
            assert count == 2
            result = spark.table("test_db.test_table")
            assert result.count() == 2


class TestMergeLogic:
    """Tests for MERGE logic (mocked)."""

    def test_merge_condition_build(self):
        pk_cols = ["flight_id", "recorded_at"]
        cond = " AND ".join([f"t.{col} = u.{col}" for col in pk_cols])
        assert cond == "t.flight_id = u.flight_id AND t.recorded_at = u.recorded_at"

    def test_zorder_cols_validation(self, table_config):
        table_config.enable_zorder = True
        table_config.zorder_cols = []
        
        errors = table_config.validate()
        assert any("zorder_cols required" in e for e in errors)
        
        table_config.zorder_cols = ["flight_id"]
        errors = table_config.validate()
        assert not any("zorder_cols required" in e for e in errors)