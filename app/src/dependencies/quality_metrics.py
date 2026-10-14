from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class QualityMetrics:
    """Saves data quality metrics to db_raw.quality_metrics."""

    def __init__(self, spark: SparkSession, raw_db: str = "db_raw"):
        self.spark = spark
        self.raw_db = raw_db

    def save(
        self,
        database: str,
        table: str,
        metrics: List[Dict],
        reference_date: str,
        technology: str = "emr"
    ):
        """
        Save quality metrics.
        
        Args:
            metrics: List of dicts with keys: metric, rule, status, failure_reason, partition
        """
        if not metrics:
            return

        rows = []
        for m in metrics:
            rows.append({
                "database": database,
                "table": table,
                "processing_timestamp": F.current_timestamp(),
                "metric": m.get("metric"),
                "rule": m.get("rule"),
                "status": m.get("status"),
                "failure_reason": m.get("failure_reason"),
                "partition": m.get("partition"),
                "technology": technology,
                "reference_date": reference_date
            })

        df = self.spark.createDataFrame(rows)
        df.write.format("delta") \
            .mode("append") \
            .partitionBy("reference_date") \
            .saveAsTable(f"{self.raw_db}.quality_metrics")
        
        logger.info(f"Saved {len(rows)} quality metrics for {database}.{table}")

    def record_check(
        self,
        database: str,
        table: str,
        metric: str,
        rule: str,
        status: str,
        failure_reason: str = None,
        partition: str = None,
        reference_date: str = None,
        technology: str = "emr"
    ):
        """Record a single quality check result."""
        self.save(database, table, [{
            "metric": metric,
            "rule": rule,
            "status": status,
            "failure_reason": failure_reason,
            "partition": partition
        }], reference_date, technology)