from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import List, Optional
import logging

from .config_models import TableConfig

logger = logging.getLogger(__name__)


class Writer:
    """Handles writing DataFrames to Delta tables with MERGE.
    
    Tables must be pre-created in Glue Catalog by infrastructure process.
    This class only manipulates data, not metadata.
    """

    def __init__(self, spark: SparkSession, trusted_db: str):
        self.spark = spark
        self.trusted_db = trusted_db

    def write(
        self,
        table: TableConfig,
        df: DataFrame,
        processing_date: Optional[str] = None,
        mode: str = "incremental"
    ) -> int:
        target_table = table.target_table
        full_name = f"{self.trusted_db}.{target_table}"
        pk_cols = table.primary_key

        logger.info(f"Writing to {full_name} (mode={mode}, pk={pk_cols})")

        # Verify table exists in Glue Catalog (read-only check)
        self._verify_table_exists(full_name)
        
        return self._merge_delta(full_name, df, pk_cols, table, processing_date, mode)

    def _verify_table_exists(self, full_name: str) -> None:
        """Verify table exists in Glue Catalog without creating it.
        
        Raises:
            RuntimeError: If table doesn't exist
        """
        try:
            DeltaTable.forName(self.spark, full_name)
        except Exception as e:
            raise RuntimeError(
                f"Table {full_name} not found in Glue Catalog. "
                "Tables must be pre-created by infrastructure process."
            ) from e

    def _merge_delta(
        self,
        full_name: str,
        updates: DataFrame,
        pk_cols: List[str],
        table: TableConfig,
        processing_date: Optional[str],
        mode: str
    ) -> int:
        """Merge updates into existing Delta table."""
        delta_table = DeltaTable.forName(self.spark, full_name)

        merge_cond = " AND ".join([f"t.{col} = u.{col}" for col in pk_cols])

        logger.info(f"Merging into {full_name} on {merge_cond}")

        merge_builder = delta_table.alias("t").merge(updates.alias("u"), merge_cond)
        merge_builder = merge_builder.whenMatchedUpdateAll() \
                                     .whenNotMatchedInsertAll()
        merge_builder.execute()

        count = updates.count()
        logger.info(f"Merged {count} records into {full_name}")

        if table.enable_zorder and table.zorder_cols:
            self._zorder(full_name, table.zorder_cols)
        
        if mode == "full":
            self._optimize(full_name)
            self._vacuum(full_name)

        return count

    def _optimize(self, full_name: str):
        logger.info(f"Running OPTIMIZE on {full_name}")
        self.spark.sql(f"OPTIMIZE {full_name}")

    def _zorder(self, full_name: str, zorder_cols: List[str]):
        cols = ", ".join(zorder_cols)
        logger.info(f"Running ZORDER BY ({cols}) on {full_name}")
        self.spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({cols})")

    def _vacuum(self, full_name: str, retention_hours: int = 168):
        logger.info(f"Running VACUUM on {full_name} (retention={retention_hours}h)")
        self.spark.sql(f"VACUUM {full_name} RETAIN {retention_hours} HOURS")

    def write_rejects(self, rejects_df: DataFrame, execution_id: str, reference_date: str):
        """Write rejected records to db_raw.rejected_records.
        
        Assumes table is pre-created in Glue Catalog.
        """
        if rejects_df is None or rejects_df.count() == 0:
            return

        full_name = "db_raw.rejected_records"
        
        # Verify table exists
        self._verify_table_exists(full_name)

        logger.info(f"Writing {rejects_df.count()} rejected records to {full_name}")
        
        enriched = rejects_df.withColumn("execution_id", F.lit(execution_id)) \
                            .withColumn("execution_timestamp", F.current_timestamp()) \
                            .withColumn("reference_date", F.lit(reference_date))

        delta_table = DeltaTable.forName(self.spark, full_name)
        
        # Use MERGE to append (pk is execution_id + reference_date)
        # Since this is append-only with unique execution_id, we can use insert only
        merge_builder = delta_table.alias("t").merge(
            enriched.alias("u"),
            "t.execution_id = u.execution_id AND t.reference_date = u.reference_date"
        )
        merge_builder.whenNotMatchedInsertAll().execute()
        
        logger.info(f"Written rejected records to {full_name}")