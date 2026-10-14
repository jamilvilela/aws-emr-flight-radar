from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class RejectedRecords:
    """Centralizes rejected records in db_raw.rejected_records."""

    def __init__(self, spark: SparkSession, raw_db: str = "db_raw"):
        self.spark = spark
        self.raw_db = raw_db

    def write(
        self,
        rejects_df: DataFrame,
        execution_id: str,
        source_database: str,
        source_table: str,
        target_database: str,
        target_table: str,
        reference_date: str
    ):
        """
        Write rejected records to centralized table.
        
        Args:
            rejects_df: DataFrame with reject_rule and reject_reason columns
            execution_id: Unique execution identifier
            source_database: Source database (e.g., db_raw)
            source_table: Source table name
            target_database: Target database (e.g., db_trusted)
            target_table: Target table name
            reference_date: Processing date for partitioning
        """
        if rejects_df is None or rejects_df.count() == 0:
            return

        enriched = rejects_df.withColumn("execution_id", F.lit(execution_id)) \
                            .withColumn("execution_timestamp", F.current_timestamp()) \
                            .withColumn("source_database", F.lit(source_database)) \
                            .withColumn("source_table", F.lit(source_table)) \
                            .withColumn("target_database", F.lit(target_database)) \
                            .withColumn("target_table", F.lit(target_table)) \
                            .withColumn("rejected_record_json", F.to_json(F.struct("*"))) \
                            .withColumn("reference_date", F.lit(reference_date))

        # Select only the columns we need for the rejected_records table
        output_cols = [
            "execution_id",
            "execution_timestamp",
            "source_database",
            "source_table",
            "target_database",
            "target_table",
            "reject_rule",
            "reject_reason",
            "rejected_record_json",
            "reference_date"
        ]
        
        output_df = enriched.select(*output_cols)
        
        output_df.write.format("delta") \
            .mode("append") \
            .partitionBy("reference_date") \
            .saveAsTable(f"{self.raw_db}.rejected_records")
        
        logger.info(f"Written {output_df.count()} rejected records for {target_database}.{target_table}")