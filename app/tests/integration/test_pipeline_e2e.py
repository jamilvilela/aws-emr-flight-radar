import pytest
import boto3
import json
import tempfile
from moto import mock_aws
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class TestPipelineE2E:
    """End-to-end integration tests for the ETL pipeline."""

    @pytest.fixture
    def setup_mock_aws(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="lakehouse-raw-123456789012")
            s3.create_bucket(Bucket="lakehouse-trusted-123456789012")
            s3.create_bucket(Bucket="lakehouse-workspace-123456789012")
            s3.create_bucket(Bucket="lakehouse-logs-123456789012")
            
            glue = boto3.client("glue", region_name="us-east-1")
            glue.create_database(DatabaseInput={"Name": "db_raw"})
            glue.create_database(DatabaseInput={"Name": "db_trusted"})
            
            yield s3, glue

    @pytest.fixture
    def sample_bronze_data(self, spark):
        aircraft_types = [
            ("B738", "738", "Boeing 737-800", "Boeing", "2024-01-01 10:00:00"),
            ("A320", "320", "Airbus A320", "Airbus", "2024-01-01 10:00:00"),
        ]
        df_at = spark.createDataFrame(aircraft_types, ["icao_code", "iata_code", "name", "manufacturer", "updated_at"])
        df_at = df_at.withColumn("cdc_operation", F.lit("U")) \
                     .withColumn("cdc_timestamp", F.lit("2024-01-01 10:00:00")) \
                     .withColumn("event_date", F.lit("2024-01-01"))
        
        aircraft = [
            ("A12345", "B738", "N12345", "Boeing 737-800", "2024-01-01 10:00:00"),
            ("B67890", "A320", "N67890", "Airbus A320", "2024-01-01 10:00:00"),
        ]
        df_ac = spark.createDataFrame(aircraft, ["icao24", "aircraft_type", "registration", "operator_name", "updated_at"])
        df_ac = df_ac.withColumn("cdc_operation", F.lit("U")) \
                     .withColumn("cdc_timestamp", F.lit("2024-01-01 10:00:00")) \
                     .withColumn("event_date", F.lit("2024-01-01")) \
                     .withColumn("created_at", F.lit("2024-01-01 10:00:00")) \
                     .withColumn("serial_number", F.lit("12345")) \
                     .withColumn("operator_icao", F.lit("SWA")) \
                     .withColumn("year_built", F.lit(2020))
        
        return {"fr_aircraft_types": df_at, "fr_aircraft": df_ac}

    @mock_aws
    def test_dim_aircraft_type_transformation(self, spark, setup_mock_aws, sample_bronze_data):
        s3, glue = setup_mock_aws
        
        from app.src.dependencies.transformer import Transformer
        from app.src.dependencies.config_models import TableConfig
        
        sample_bronze_data["fr_aircraft_types"].createOrReplaceTempView("db_raw.fr_aircraft_types")
        
        transformer = Transformer(spark, "db_raw", "db_trusted")
        
        table = TableConfig(
            target_table="dim_aircraft_type",
            source_tables=["fr_aircraft_types"],
            primary_key=["icao_code"],
            surrogate_key={"name": "aircraft_type_key", "col": "icao_code"},
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="spark_sql",
            transform_query="SELECT xxhash64(icao_code) AS aircraft_type_key, icao_code, iata_code, name AS type_name, manufacturer, updated_at, current_timestamp() AS ingestion_ts, 'EMR_SPARK' AS process_engine, date(coalesce(updated_at, created_at)) AS event_date FROM db_raw.fr_aircraft_types WHERE cdc_operation <> 'D' QUALIFY ROW_NUMBER() OVER (PARTITION BY icao_code ORDER BY updated_at DESC NULLS LAST) = 1",
            order=1
        )
        
        result = transformer.transform(table, processing_date=None, mode="full")
        
        assert result.count() == 2
        assert "aircraft_type_key" in result.columns
        assert "ingestion_ts" in result.columns
        assert "process_engine" in result.columns
        assert "event_date" in result.columns
        
        keys = [row.aircraft_type_key for row in result.select("aircraft_type_key").collect()]
        assert all(k is not None for k in keys)
        assert len(set(keys)) == 2

    @mock_aws
    def test_dim_aircraft_transformation_with_join(self, spark, setup_mock_aws, sample_bronze_data):
        s3, glue = setup_mock_aws
        
        from app.src.dependencies.transformer import Transformer
        from app.src.dependencies.config_models import TableConfig
        
        sample_bronze_data["fr_aircraft_types"].createOrReplaceTempView("db_raw.fr_aircraft_types")
        sample_bronze_data["fr_aircraft"].createOrReplaceTempView("db_raw.fr_aircraft")
        
        transformer = Transformer(spark, "db_raw", "db_trusted")
        
        table = TableConfig(
            target_table="dim_aircraft",
            source_tables=["fr_aircraft", "fr_aircraft_types"],
            primary_key=["icao24"],
            surrogate_key={"name": "aircraft_key", "col": "icao24"},
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="spark_sql",
            transform_query="SELECT xxhash64(a.icao24) AS aircraft_key, a.icao24, a.registration, a.aircraft_type, at.iata_code AS type_iata_code, at.name AS type_name, at.manufacturer, a.serial_number, a.operator_icao, a.operator_name, a.year_built, a.created_at, a.updated_at, current_timestamp() AS ingestion_ts, 'EMR_SPARK' AS process_engine, date(coalesce(a.updated_at, a.created_at)) AS event_date FROM db_raw.fr_aircraft a LEFT JOIN db_raw.fr_aircraft_types at ON a.aircraft_type = at.icao_code WHERE a.cdc_operation <> 'D' QUALIFY ROW_NUMBER() OVER (PARTITION BY a.icao24 ORDER BY a.updated_at DESC NULLS LAST) = 1",
            dependencies=["dim_aircraft_type"],
            order=2
        )
        
        result = transformer.transform(table, processing_date=None, mode="full")
        
        assert result.count() == 2
        assert "aircraft_key" in result.columns
        assert "type_iata_code" in result.columns
        assert "type_name" in result.columns
        
        rows = result.collect()
        for row in rows:
            assert row.type_iata_code is not None
            assert row.type_name is not None

    @mock_aws
    def test_fact_flight_transformation(self, spark, setup_mock_aws):
        s3, glue = setup_mock_aws
        
        from app.src.jobs.process_fact_flight import transform_fact_flight
        
        airline_data = [("SWA", "WN", "Southwest Airlines", "2024-01-01")]
        df_airline = spark.createDataFrame(airline_data, ["icao_code", "iata_code", "name", "updated_at"])
        df_airline = df_airline.withColumn("airline_key", F.xxhash64(F.col("icao_code")).cast("bigint")) \
                               .withColumn("event_date", F.lit("2024-01-01")) \
                               .withColumn("ingestion_ts", F.current_timestamp()) \
                               .withColumn("process_engine", F.lit("EMR_SPARK"))
        df_airline.createOrReplaceTempView("db_trusted.dim_airline")
        
        aircraft_data = [("A12345", "B738", "N12345"), ("B67890", "A320", "N67890")]
        df_aircraft = spark.createDataFrame(aircraft_data, ["icao24", "aircraft_type", "registration"])
        df_aircraft = df_aircraft.withColumn("aircraft_key", F.xxhash64(F.col("icao24")).cast("bigint")) \
                                 .withColumn("event_date", F.lit("2024-01-01")) \
                                 .withColumn("ingestion_ts", F.current_timestamp()) \
                                 .withColumn("process_engine", F.lit("EMR_SPARK"))
        df_aircraft.createOrReplaceTempView("db_trusted.dim_aircraft")
        
        airport_data = [("KLAX", "LAX", "Los Angeles Intl"), ("KSFO", "SFO", "San Francisco Intl")]
        df_airport = spark.createDataFrame(airport_data, ["icao_code", "iata_code", "name"])
        df_airport = df_airport.withColumn("airport_key", F.xxhash64(F.col("icao_code")).cast("bigint")) \
                               .withColumn("event_date", F.lit("2024-01-01")) \
                               .withColumn("ingestion_ts", F.current_timestamp()) \
                               .withColumn("process_engine", F.lit("EMR_SPARK"))
        df_airport.createOrReplaceTempView("db_trusted.dim_airport")
        
        route_data = [(1, "SWA", "KLAX", "KSFO", "2024-01-01")]
        df_route = spark.createDataFrame(route_data, ["id", "airline_iata", "src_airport", "dst_airport", "updated_at"])
        df_route = df_route.withColumn("route_key", F.xxhash64(F.col("id")).cast("bigint")) \
                          .withColumn("origin_icao", F.col("src_airport")) \
                          .withColumn("destination_icao", F.col("dst_airport")) \
                          .withColumn("event_date", F.lit("2024-01-01")) \
                          .withColumn("ingestion_ts", F.current_timestamp()) \
                          .withColumn("process_engine", F.lit("EMR_SPARK"))
        df_route.createOrReplaceTempView("db_trusted.dim_route")
        
        flights_data = [
            (1001, "WN123", "SWA", "A12345", "KLAX", "KSFO", "2024-01-15 10:00:00", "2024-01-15 12:00:00", "active", "2024-01-15 09:00:00"),
        ]
        df_flights = spark.createDataFrame(flights_data, ["flight_id", "flight_number", "airline_icao", "aircraft_icao24", "origin_airport", "destination_airport", "scheduled_departure", "scheduled_arrival", "status", "source_ingestion_ts"])
        df_flights = df_flights.withColumn("cdc_operation", F.lit("U")) \
                              .withColumn("event_date", F.to_date(F.col("scheduled_departure")))
        df_flights.createOrReplaceTempView("db_raw.fr_flights")
        
        result = transform_fact_flight(spark, "db_raw", "db_trusted", "2024-01-15", "incremental")
        
        assert result.count() == 1
        row = result.collect()[0]
        assert row.flight_id == 1001
        assert row.airline_key is not None
        assert row.aircraft_key is not None
        assert row.origin_airport_key is not None
        assert row.destination_airport_key is not None
        assert row.route_key is not None
        assert row.event_date == "2024-01-15"
        assert row.process_engine == "EMR_SPARK"