import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.src.dependencies.data_quality import DataQuality
from app.src.dependencies.config_models import TableConfig


class TestDataQuality:
    """Tests for data_quality.py"""

    @pytest.fixture
    def sample_df(self, spark):
        data = [
            (1, "A123", "active", "2024-01-01"),
            (2, None, "active", "2024-01-01"),
            (3, "B456", "invalid_status", "2024-01-01"),
            (4, "C789", "landed", "2024-01-01"),
        ]
        return spark.createDataFrame(data, ["id", "flight_id", "status", "event_date"])

    @pytest.fixture
    def table_config(self):
        return TableConfig(
            target_table="fact_flight",
            source_tables=["fr_flights"],
            primary_key=["flight_id"],
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="pyspark",
            transform_module="jobs.process_fact_flight",
            enum_columns={"status": ["scheduled", "active", "landed", "cancelled", "diverted"]},
            order=1
        )

    def test_check_pk_not_null(self, spark, sample_df, table_config):
        dq = DataQuality(spark, "db_trusted")
        
        valid_df, rejects_df = dq.run_checks(sample_df, table_config, "exec-123", "2024-01-01")
        
        assert valid_df.count() == 3
        assert rejects_df.count() == 1
        reject_reason = rejects_df.select("reject_reason").collect()[0][0]
        assert "Primary key column is null" in reject_reason

    def test_check_enums(self, spark, sample_df, table_config):
        dq = DataQuality(spark, "db_trusted")
        
        valid_df, rejects_df = dq.run_checks(sample_df, table_config, "exec-123", "2024-01-01")
        
        valid_statuses = [row.status for row in valid_df.select("status").collect()]
        assert "invalid_status" not in valid_statuses
        assert rejects_df.filter(F.col("reject_rule") == "enum_validation").count() >= 1

    def test_check_cdc_operation(self, spark, table_config):
        dq = DataQuality(spark, "db_trusted")
        
        data = [
            (1, "A123", "U"),
            (2, "B456", "D"),
            (3, "C789", "I"),
        ]
        df = spark.createDataFrame(data, ["id", "flight_id", "cdc_operation"])
        
        valid_df, rejects_df = dq.run_checks(df, table_config, "exec-123", "2024-01-01")
        
        assert valid_df.count() == 2
        assert rejects_df.count() == 1
        reject_reason = rejects_df.select("reject_reason").collect()[0][0]
        assert "Delete" in reject_reason

    def test_no_enum_columns(self, spark):
        dq = DataQuality(spark, "db_trusted")
        
        table_no_enum = TableConfig(
            target_table="dim_aircraft",
            source_tables=["fr_aircraft"],
            primary_key=["icao24"],
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="spark_sql",
            transform_query="sql/test.sql",
            order=1
        )
        
        data = [(1, "A123"), (2, "B456")]
        df = spark.createDataFrame(data, ["id", "icao24"])
        
        valid_df, rejects_df = dq.run_checks(df, table_no_enum, "exec-123", "2024-01-01")
        
        assert valid_df.count() == 2
        assert rejects_df.count() == 0