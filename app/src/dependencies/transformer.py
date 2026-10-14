from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional
import importlib
import logging

from .config_models import TableConfig

logger = logging.getLogger(__name__)


class Transformer:
    """Handles Spark SQL and PySpark transformations for target tables."""

    def __init__(self, spark: SparkSession, raw_db: str, trusted_db: str):
        self.spark = spark
        self.raw_db = raw_db
        self.trusted_db = trusted_db

    def transform(
        self,
        table: TableConfig,
        processing_date: Optional[str] = None,
        mode: str = "incremental"
    ) -> DataFrame:
        logger.info(f"Transforming table: {table.target_table} (type={table.transform_type})")

        if table.transform_type == "spark_sql":
            return self._transform_spark_sql(table, processing_date, mode)
        elif table.transform_type == "pyspark":
            return self._transform_pyspark(table, processing_date, mode)
        else:
            raise ValueError(f"Unknown transform_type: {table.transform_type}")

    def _transform_spark_sql(
        self,
        table: TableConfig,
        processing_date: Optional[str],
        mode: str
    ) -> DataFrame:
        if not table.transform_query:
            raise ValueError(f"transform_query required for {table.target_table}")

        sql_path = table.transform_query
        
        try:
            with open(sql_path, "r") as f:
                sql = f.read()
        except FileNotFoundError:
            sql = self._read_sql_from_s3(sql_path)

        if mode == "incremental" and processing_date:
            partition_col = "event_date"
            if table.target_table == "fact_flight_position":
                partition_col = "recorded_at"
            sql = f"""
            WITH filtered AS (
                {sql}
            )
            SELECT * FROM filtered WHERE {partition_col} = '{processing_date}'
            """

        logger.debug(f"Executing SQL for {table.target_table}: {sql[:200]}...")
        return self.spark.sql(sql)

    def _transform_pyspark(
        self,
        table: TableConfig,
        processing_date: Optional[str],
        mode: str
    ) -> DataFrame:
        if not table.transform_module:
            raise ValueError(f"transform_module required for {table.target_table}")

        module_name = table.transform_module
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            raise ImportError(f"Failed to import transform module {module_name}: {e}")

        func_name = f"transform_{table.target_table}"
        if not hasattr(module, func_name):
            func_name = f"transform_{table.target_table.replace('dim_', '').replace('fact_', '')}"
            if not hasattr(module, func_name):
                raise AttributeError(f"Module {module_name} has no function {func_name}")

        transform_func = getattr(module, func_name)
        return transform_func(self.spark, self.raw_db, self.trusted_db, processing_date, mode)

    def _read_sql_from_s3(self, s3_path: str) -> str:
        import boto3
        
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Expected s3:// path for SQL file, got: {s3_path}")
        
        bucket = s3_path.split("/")[2]
        key = "/".join(s3_path.split("/")[3:])
        
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")


def surrogate_key(col_name: str) -> str:
    from pyspark.sql import functions as F
    return F.xxhash64(F.col(col_name)).cast("bigint")