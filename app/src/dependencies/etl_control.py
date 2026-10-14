from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional
import uuid
import logging

logger = logging.getLogger(__name__)


class EtlControl:
    """Manages ETL execution tracking in db_raw.etl_control."""

    def __init__(self, spark: SparkSession, raw_db: str = "db_raw"):
        self.spark = spark
        self.raw_db = raw_db
        self.execution_id = str(uuid.uuid4())

    def register_start(
        self,
        job_name: str,
        source: str,
        target_table: str,
        mode: str,
        reference_date: str
    ):
        """Register start of ETL execution."""
        self._write_control(
            job_name=job_name,
            source=source,
            target_table=target_table,
            mode=mode,
            execution_start=F.current_timestamp(),
            execution_end=None,
            status="running",
            records_read=0,
            records_written=0,
            records_rejected=0,
            reference_date=reference_date,
            error_message=None
        )

    def register_end(
        self,
        job_name: str,
        source: str,
        target_table: str,
        mode: str,
        reference_date: str,
        status: str,
        records_read: int,
        records_written: int,
        records_rejected: int,
        error_message: Optional[str] = None
    ):
        """Register end of ETL execution."""
        self._write_control(
            job_name=job_name,
            source=source,
            target_table=target_table,
            mode=mode,
            execution_start=None,  # Will be filled from start record
            execution_end=F.current_timestamp(),
            status=status,
            records_read=records_read,
            records_written=records_written,
            records_rejected=records_rejected,
            reference_date=reference_date,
            error_message=error_message
        )

    def _write_control(self, **kwargs):
        """Write control record to db_raw.etl_control."""
        df = self.spark.createDataFrame([kwargs])
        df.write.format("delta") \
            .mode("append") \
            .partitionBy("reference_date") \
            .saveAsTable(f"{self.raw_db}.etl_control")