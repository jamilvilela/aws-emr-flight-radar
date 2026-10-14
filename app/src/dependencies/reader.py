from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def read_table(
    spark,
    database: str,
    table: str,
    processing_date: Optional[str] = None,
    mode: str = "incremental",
    partition_column: str = "event_date"
) -> DataFrame:
    full_name = f"{database}.{table}"
    logger.info(f"Reading table: {full_name} (mode={mode}, partition={partition_column})")

    df = spark.table(full_name)

    if mode == "incremental" and processing_date:
        if partition_column == "recorded_at":
            df = df.filter(F.to_date(F.col("recorded_at")) == processing_date)
        else:
            df = df.filter(F.col(partition_column) == processing_date)
        logger.info(f"Applied partition filter: {partition_column} = {processing_date}")
    elif mode == "full" and partition_column == "recorded_at":
        df = df.filter(F.col("recorded_at") >= F.date_sub(F.current_date(), 730))
        logger.info("Applied full mode range filter: recorded_at >= current_date - 730 days")

    df = _rename_cdc_columns(df)

    return df


def _rename_cdc_columns(df: DataFrame) -> DataFrame:
    rename_map = {
        "Op": "cdc_operation",
        "dms_timestamp": "cdc_timestamp",
        "op": "cdc_operation",
    }
    
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df


def get_partition_column(target_table: str) -> str:
    position_tables = ["fact_flight_position"]
    if target_table in position_tables:
        return "recorded_at"
    return "event_date"