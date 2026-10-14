from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def transform_fact_flight(spark, raw_db: str, trusted_db: str, processing_date: str, mode: str) -> DataFrame:
    """
    Transform fact_flight by joining with dimensions to get surrogate keys.
    """
    flights = spark.table(f"{raw_db}.fr_flights").filter(F.col("cdc_operation") != "D")

    if mode == "incremental":
        flights = flights.filter(F.col("event_date") == processing_date)

    dim_airline = spark.table(f"{trusted_db}.dim_airline")
    dim_aircraft = spark.table(f"{trusted_db}.dim_aircraft")
    dim_airport = spark.table(f"{trusted_db}.dim_airport")
    dim_route = spark.table(f"{trusted_db}.dim_route")

    orig = dim_airport.alias("orig")
    dest = dim_airport.alias("dest")

    fact = (flights
        .join(dim_airline, flights.airline_icao == dim_airline.icao_code, "left")
        .join(dim_aircraft, flights.aircraft_icao24 == dim_aircraft.icao24, "left")
        .join(orig, flights.origin_airport == orig.icao_code, "left")
        .join(dest, flights.destination_airport == dest.icao_code, "left")
        .join(
            dim_route,
            (orig.icao_code == dim_route.origin_icao)
                & (dest.icao_code == dim_route.destination_icao),
            "left",
        )
        .select(
            F.col("flight_id"),
            F.col("flight_number"),
            dim_airline.airline_key,
            dim_aircraft.aircraft_key,
            orig.airport_key.alias("origin_airport_key"),
            dest.airport_key.alias("destination_airport_key"),
            dim_route.route_key,
            F.col("scheduled_departure"),
            F.col("scheduled_arrival"),
            F.col("actual_departure"),
            F.col("actual_arrival"),
            F.col("status"),
            F.col("source_ingestion_ts"),
            F.current_timestamp().alias("trusted_ingestion_ts"),
            F.lit("EMR_SPARK").alias("trusted_process_engine"),
            F.to_date(F.col("scheduled_departure")).alias("event_date"),
        )
    )

    return fact