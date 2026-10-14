from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def transform_dim_date(spark, raw_db: str, trusted_db: str, processing_date: str, mode: str) -> DataFrame:
    """
    Generate dim_date table - calendar dimension.
    Generated for full mode (range configurable, e.g., 2020-01-01 to +2 years).
    In incremental mode, generates only the processing_date.
    """
    if mode == "full":
        # Default range: 2020-01-01 to 2 years from now
        start_date = "2020-01-01"
        end_date = F.date_add(F.current_date(), 730)  # ~2 years
    else:
        # Incremental: just the processing_date
        start_date = processing_date
        end_date = processing_date

    # Generate date range using sequence
    date_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
    """)

    dim_date = date_df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        (F.dayofweek("date").isin([1, 7])).alias("is_weekend"),
        F.col("date").alias("event_date")
    )

    return dim_date