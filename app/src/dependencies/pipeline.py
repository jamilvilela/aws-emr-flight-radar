from pyspark.sql import SparkSession
from typing import List, Dict, Any
import logging
import time

from dependencies.config_models import TablesConfig, TableConfig
from dependencies.reader import read_table, get_partition_column
from dependencies.transformer import Transformer
from dependencies.writer import Writer
from dependencies.data_quality import DataQuality
from dependencies.etl_control import EtlControl
from dependencies.quality_metrics import QualityMetrics
from dependencies.rejected_records import RejectedRecords
from dependencies.aws_helper import put_metric

logger = logging.getLogger(__name__)


class Pipeline:
    """Encapsulates the complete ETL pipeline for a single table."""

    def __init__(
        self,
        spark: SparkSession,
        config: TablesConfig,
        raw_db: str,
        trusted_db: str,
        mode: str,
        processing_date: str
    ):
        self.spark = spark
        self.config = config
        self.raw_db = raw_db
        self.trusted_db = trusted_db
        self.mode = mode
        self.processing_date = processing_date

        self.transformer = Transformer(spark, raw_db, trusted_db)
        self.writer = Writer(spark, trusted_db)
        self.dq = DataQuality(spark, trusted_db)
        self.etl_control = EtlControl(spark, raw_db)
        self.quality_metrics = QualityMetrics(spark, raw_db)
        self.rejected_records = RejectedRecords(spark, raw_db)

    def run(self, table_name: str) -> Dict[str, Any]:
        """Run ETL pipeline for a single table."""
        logger.info(f"=== Processing table: {table_name} ===")
        start_time = time.time()

        table_config = self.config.get_table(table_name)
        if not table_config:
            raise ValueError(f"Table config not found: {table_name}")

        partition_col = get_partition_column(table_name)
        reference_date = self.processing_date
        source_tables = table_config.source_tables
        execution_id = self.etl_control.execution_id

        self.etl_control.register_start(
            job_name="emr-flight-radar",
            source=",".join(source_tables) if source_tables else "generated",
            target_table=table_name,
            mode=self.mode,
            reference_date=reference_date
        )

        records_read = 0
        records_written = 0
        records_rejected = 0
        error_message = None
        status = "success"

        try:
            logger.info(f"Reading source tables: {source_tables}")
            if source_tables:
                primary_source = source_tables[0]
                raw_df = read_table(
                    self.spark,
                    self.raw_db,
                    primary_source,
                    self.processing_date,
                    self.mode,
                    partition_col
                )
                records_read = raw_df.count()
                logger.info(f"Read {records_read} records from {primary_source}")
            else:
                raw_df = None

            logger.info(f"Transforming {table_name}")
            transformed_df = self.transformer.transform(table_config, self.processing_date, self.mode)

            logger.info(f"Running data quality checks for {table_name}")
            valid_df, rejects_df = self.dq.run_checks(
                transformed_df,
                table_config,
                execution_id,
                reference_date
            )

            records_rejected = rejects_df.count()

            if records_rejected > 0:
                self.rejected_records.write(
                    rejects_df,
                    execution_id,
                    self.raw_db,
                    source_tables[0] if source_tables else "generated",
                    self.trusted_db,
                    table_name,
                    reference_date
                )

            logger.info(f"Writing {table_name} to target")
            records_written = self.writer.write(table_config, valid_df, self.processing_date, self.mode)

            if table_config.enable_zorder and table_config.zorder_cols:
                logger.info(f"ZORDER optimization for {table_name}")
                self.writer._zorder(f"{self.trusted_db}.{table_name}", table_config.zorder_cols)

            duration = time.time() - start_time
            logger.info(f"Completed {table_name}: read={records_read}, written={records_written}, rejected={records_rejected}, duration={duration:.1f}s")

        except Exception as e:
            error_message = str(e)
            status = "failed"
            logger.error(f"Failed to process {table_name}: {error_message}")
            raise

        finally:
            self.etl_control.register_end(
                job_name="emr-flight-radar",
                source=",".join(source_tables) if source_tables else "generated",
                target_table=table_name,
                mode=self.mode,
                reference_date=reference_date,
                status=status,
                records_read=records_read,
                records_written=records_written,
                records_rejected=records_rejected,
                error_message=error_message
            )

            self.quality_metrics.save(
                database=self.trusted_db,
                table=table_name,
                metrics=[
                    {"metric": "records_read", "rule": "count", "status": "success", "partition": reference_date},
                    {"metric": "records_written", "rule": "count", "status": "success", "partition": reference_date},
                    {"metric": "records_rejected", "rule": "count", "status": "success" if records_rejected == 0 else "warning", "partition": reference_date},
                ],
                reference_date=reference_date
            )

            duration = time.time() - start_time
            put_metric("FlightRadar", "ETL.RowsRead", records_read, dimensions={"Table": table_name})
            put_metric("FlightRadar", "ETL.RowsWritten", records_written, dimensions={"Table": table_name})
            put_metric("FlightRadar", "ETL.RowsRejected", records_rejected, dimensions={"Table": table_name})
            put_metric("FlightRadar", "ETL.Duration", duration, unit="Seconds", dimensions={"Table": table_name})

        return {
            "table": table_name,
            "status": status,
            "records_read": records_read,
            "records_written": records_written,
            "records_rejected": records_rejected,
            "error": error_message,
            "duration_seconds": time.time() - start_time
        }


class PipelineRunner:
    """Orchestrates pipeline execution for multiple tables."""

    def __init__(
        self,
        spark: SparkSession,
        config: TablesConfig,
        raw_db: str,
        trusted_db: str,
        mode: str,
        processing_date: str
    ):
        self.spark = spark
        self.config = config
        self.raw_db = raw_db
        self.trusted_db = trusted_db
        self.mode = mode
        self.processing_date = processing_date

    def run(self, tables_arg: str) -> List[Dict[str, Any]]:
        tables_to_process = self.config.resolve_tables(tables_arg)
        logger.info(f"Tables to process: {[t.target_table for t in tables_to_process]}")

        pipeline = Pipeline(
            self.spark, self.config, self.raw_db, self.trusted_db,
            self.mode, self.processing_date
        )

        results = []
        for table_config in tables_to_process:
            result = pipeline.run(table_config.target_table)
            results.append(result)

        logger.info("=== ETL Summary ===")
        for r in results:
            logger.info(f"  {r['table']}: {r['status']} (read={r['records_read']}, written={r['records_written']}, rejected={r['records_rejected']}, duration={r['duration_seconds']:.1f}s)")

        failed = [r for r in results if r["status"] == "failed"]
        if failed:
            logger.error(f"Failed tables: {[r['table'] for r in failed]}")
            raise RuntimeError(f"Pipeline failed for tables: {[r['table'] for r in failed]}")

        logger.info("All tables processed successfully")
        return results