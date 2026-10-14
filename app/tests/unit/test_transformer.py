import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.src.dependencies.transformer import Transformer, surrogate_key
from app.src.dependencies.config_models import TableConfig


class TestTransformer:
    """Tests for transformer.py"""

    def test_surrogate_key(self):
        from pyspark.sql import Column
        result = surrogate_key("icao24")
        assert isinstance(result, Column)

    def test_transformer_init(self, spark):
        transformer = Transformer(spark, "db_raw", "db_trusted")
        assert transformer.raw_db == "db_raw"
        assert transformer.trusted_db == "db_trusted"


class TestSparkSQLTransform:
    """Tests for Spark SQL transformations (require temp views)."""

    def test_transform_spark_sql_simple(self, spark):
        data = [
            ("ABC123", "Boeing", "737", "2024-01-01"),
            ("DEF456", "Airbus", "A320", "2024-01-02"),
        ]
        df = spark.createDataFrame(data, ["icao_code", "manufacturer", "name", "updated_at"])
        df.createOrReplaceTempView("test_source")

        transformer = Transformer(spark, "db_raw", "db_trusted")
        
        table = TableConfig(
            target_table="test_table",
            source_tables=["test_source"],
            primary_key=["icao_code"],
            surrogate_key={"name": "test_key", "col": "icao_code"},
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="spark_sql",
            transform_query="SELECT icao_code, manufacturer, name, updated_at, current_timestamp() as ingestion_ts, 'EMR_SPARK' as process_engine, date(updated_at) as event_date FROM test_source",
            order=1
        )

        result = transformer.transform(table, processing_date=None, mode="full")
        assert result.count() == 2
        assert "test_key" in result.columns
        assert "ingestion_ts" in result.columns


class TestPySparkTransform:
    """Tests for PySpark transformations."""

    def test_transform_pyspark_import_error(self, spark):
        transformer = Transformer(spark, "db_raw", "db_trusted")
        
        table = TableConfig(
            target_table="test_table",
            source_tables=[],
            primary_key=["id"],
            transform_type="pyspark",
            transform_module="nonexistent.module",
            order=1
        )

        with pytest.raises(ImportError):
            transformer.transform(table)