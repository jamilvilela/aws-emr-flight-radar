import pytest
from pyspark.sql import SparkSession

from app.src.dependencies.config_models import TablesConfig, TableConfig


class TestConfigModels:
    """Tests for config_models.py"""

    def test_table_config_validation_valid(self):
        table = TableConfig(
            target_table="dim_aircraft",
            source_tables=["fr_aircraft", "fr_aircraft_types"],
            primary_key=["icao24"],
            surrogate_key={"name": "aircraft_key", "col": "icao24"},
            partition_keys=[{"name": "event_date", "type": "date"}],
            transform_type="spark_sql",
            transform_query="sql/dim_aircraft.sql",
            dependencies=["dim_aircraft_type"],
            order=2
        )
        errors = table.validate()
        assert errors == []

    def test_table_config_validation_missing_target_table(self):
        table = TableConfig(
            target_table="",
            source_tables=["fr_aircraft"],
            primary_key=["icao24"],
            transform_type="spark_sql",
            transform_query="sql/dim_aircraft.sql",
            order=1
        )
        errors = table.validate()
        assert "target_table is required" in errors

    def test_table_config_validation_missing_primary_key(self):
        table = TableConfig(
            target_table="dim_aircraft",
            source_tables=["fr_aircraft"],
            primary_key=[],
            transform_type="spark_sql",
            transform_query="sql/dim_aircraft.sql",
            order=1
        )
        errors = table.validate()
        assert "primary_key is required" in errors

    def test_table_config_validation_invalid_transform_type(self):
        table = TableConfig(
            target_table="dim_aircraft",
            source_tables=["fr_aircraft"],
            primary_key=["icao24"],
            transform_type="invalid",
            order=1
        )
        errors = table.validate()
        assert any("transform_type must be" in e for e in errors)

    def test_table_config_validation_missing_transform_query(self):
        table = TableConfig(
            target_table="dim_aircraft",
            source_tables=["fr_aircraft"],
            primary_key=["icao24"],
            transform_type="spark_sql",
            transform_query=None,
            order=1
        )
        errors = table.validate()
        assert any("transform_query required" in e for e in errors)

    def test_table_config_validation_missing_transform_module(self):
        table = TableConfig(
            target_table="fact_flight",
            source_tables=["fr_flights"],
            primary_key=["flight_id"],
            transform_type="pyspark",
            transform_module=None,
            order=1
        )
        errors = table.validate()
        assert any("transform_module required" in e for e in errors)

    def test_table_config_validation_zorder_requires_cols(self):
        table = TableConfig(
            target_table="fact_flight",
            source_tables=["fr_flights"],
            primary_key=["flight_id"],
            transform_type="pyspark",
            transform_module="jobs.process_fact_flight",
            enable_zorder=True,
            zorder_cols=[],
            order=1
        )
        errors = table.validate()
        assert any("zorder_cols required" in e for e in errors)

    def test_tables_config_from_dict(self):
        data = [
            {
                "target_table": "dim_aircraft_type",
                "source_tables": ["fr_aircraft_types"],
                "primary_key": ["icao_code"],
                "surrogate_key": {"name": "aircraft_type_key", "col": "icao_code"},
                "partition_keys": [{"name": "event_date", "type": "date"}],
                "transform_type": "spark_sql",
                "transform_query": "sql/dim_aircraft_type.sql",
                "dependencies": [],
                "order": 1
            },
            {
                "target_table": "dim_aircraft",
                "source_tables": ["fr_aircraft", "fr_aircraft_types"],
                "primary_key": ["icao24"],
                "surrogate_key": {"name": "aircraft_key", "col": "icao24"},
                "partition_keys": [{"name": "event_date", "type": "date"}],
                "transform_type": "spark_sql",
                "transform_query": "sql/dim_aircraft.sql",
                "dependencies": ["dim_aircraft_type"],
                "order": 2
            }
        ]

        config = TablesConfig.from_dict(data)
        assert len(config._tables) == 2
        assert config.get_table("dim_aircraft_type") is not None
        assert config.get_table("dim_aircraft") is not None

    def test_tables_config_duplicate_target_table(self):
        data = [
            {"target_table": "dim_aircraft", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 1},
            {"target_table": "dim_aircraft", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 2},
        ]
        with pytest.raises(ValueError, match="Duplicate target_table"):
            TablesConfig.from_dict(data)

    def test_tables_config_duplicate_order(self):
        data = [
            {"target_table": "dim_a", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 1},
            {"target_table": "dim_b", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 1},
        ]
        with pytest.raises(ValueError, match="Duplicate order"):
            TablesConfig.from_dict(data)

    def test_tables_config_invalid_dependency(self):
        data = [
            {"target_table": "dim_aircraft", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "dependencies": ["unknown_table"], "order": 1},
        ]
        with pytest.raises(ValueError, match="depends on unknown table"):
            TablesConfig.from_dict(data)

    def test_tables_config_resolve_tables_all(self):
        data = [
            {"target_table": "dim_a", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 1},
            {"target_table": "dim_b", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 2},
        ]
        config = TablesConfig.from_dict(data)
        tables = config.resolve_tables("all")
        assert len(tables) == 2
        assert tables[0].target_table == "dim_a"
        assert tables[1].target_table == "dim_b"

    def test_tables_config_resolve_tables_with_deps(self):
        data = [
            {"target_table": "dim_a", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "order": 1},
            {"target_table": "dim_b", "source_tables": [], "primary_key": ["id"], "transform_type": "spark_sql", "transform_query": "sql/test.sql", "dependencies": ["dim_a"], "order": 2},
        ]
        config = TablesConfig.from_dict(data)
        tables = config.resolve_tables("dim_b")
        assert len(tables) == 2
        assert tables[0].target_table == "dim_a"
        assert tables[1].target_table == "dim_b"

    def test_tables_config_to_dict(self):
        data = [
            {
                "target_table": "dim_aircraft",
                "source_tables": ["fr_aircraft"],
                "primary_key": ["icao24"],
                "surrogate_key": {"name": "aircraft_key", "col": "icao24"},
                "partition_keys": [{"name": "event_date", "type": "date"}],
                "transform_type": "spark_sql",
                "transform_query": "sql/dim_aircraft.sql",
                "dependencies": [],
                "order": 1,
                "enable_zorder": False,
                "zorder_cols": [],
                "enum_columns": {}
            }
        ]
        config = TablesConfig.from_dict(data)
        output = config.to_dict()
        assert len(output) == 1
        assert output[0]["target_table"] == "dim_aircraft"
        assert output[0]["surrogate_key"]["name"] == "aircraft_key"