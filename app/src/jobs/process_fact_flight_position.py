from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def transform_fact_flight_position(spark, raw_db: str, trusted_db: str, processing_date: str, mode: str) -> DataFrame:
    """
    Transform fact_flight_position by joining with dim_aircraft to get aircraft_key.
    Bronze table fr_aircraft_positions is partitioned by aircraft_icao24.
    """
    positions = spark.table(f"{raw_db}.fr_aircraft_positions").filter(F.col("cdc_operation") != "D")

    if mode == "incremental":
        # Partition prune by recorded_at range (not event_date)
        # In incremental mode, process only the processing_date's positions
        positions = positions.filter(F.to_date(F.col("recorded_at")) == processing_date)
    else:
        # Full mode: limit to last 2 years to avoid full scan of massive table
        positions = positions.filter(F.col("recorded_at") >= F.date_sub(F.current_date(), 730))

    dim_aircraft = spark.table(f"{trusted_db}.dim_aircraft")

    fact = (positions
        .join(dim_aircraft, positions.aircraft_icao24 == dim_aircraft.icao24, "left")
        .select(
            F.col("position_id"),
            F.col("recorded_at"),
            F.col("aircraft_icao24"),
            dim_aircraft.aircraft_key,
            F.col("flight_id"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("altitude_ft"),
            F.col("velocity_kts"),
            F.col("heading"),
            F.col("vertical_rate_fpm"),
            F.col("on_ground"),
            F.col("ingested_at"),
            F.current_timestamp().alias("trusted_ingestion_ts"),
            F.lit("EMR_SPARK").alias("trusted_process_engine"),
            F.to_date(F.col("recorded_at")).alias("event_date"),
        )
    )

    return fact