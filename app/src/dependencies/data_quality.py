from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List, Tuple, Dict, Any
import logging

from .config_models import TableConfig

logger = logging.getLogger(__name__)


class DataQuality:
    """Data quality checks for target tables."""

    def __init__(self, spark: SparkSession, trusted_db: str):
        self.spark = spark
        self.trusted_db = trusted_db

    def run_checks(
        self,
        df: DataFrame,
        table: TableConfig,
        execution_id: str,
        reference_date: str
    ) -> Tuple[DataFrame, DataFrame]:
        logger.info(f"Running DQ checks for {table.target_table}")
        
        valid_df = df
        all_rejects = []

        valid_df, rejects = self._check_pk_not_null(valid_df, table, execution_id, reference_date)
        all_rejects.append(rejects)

        valid_df, rejects = self._check_cdc_operation(valid_df, table, execution_id, reference_date)
        all_rejects.append(rejects)

        if table.enum_columns:
            valid_df, rejects = self._check_enums(valid_df, table, execution_id, reference_date)
            all_rejects.append(rejects)

        if table.target_table.startswith("fact_"):
            valid_df, rejects = self._check_fk_integrity(valid_df, table, execution_id, reference_date)
            all_rejects.append(rejects)

        if all_rejects:
            from functools import reduce
            rejects_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_rejects)
        else:
            rejects_df = self.spark.createDataFrame([], df.schema.add("reject_rule", "string").add("reject_reason", "string"))

        valid_count = valid_df.count()
        reject_count = rejects_df.count()
        logger.info(f"DQ results for {table.target_table}: valid={valid_count}, rejected={reject_count}")

        return valid_df, rejects_df

    def _check_pk_not_null(
        self,
        df: DataFrame,
        table: TableConfig,
        execution_id: str,
        reference_date: str
    ) -> Tuple[DataFrame, DataFrame]:
        pk_cols = table.primary_key
        
        null_condition = None
        for col in pk_cols:
            cond = F.col(col).isNull()
            null_condition = cond if null_condition is None else (null_condition | cond)
        
        valid_df = df.filter(~null_condition)
        rejects_df = df.filter(null_condition).withColumn("reject_rule", F.lit("pk_not_null")) \
                                                  .withColumn("reject_reason", F.lit("Primary key column is null"))
        
        return valid_df, rejects_df

    def _check_cdc_operation(
        self,
        df: DataFrame,
        table: TableConfig,
        execution_id: str,
        reference_date: str
    ) -> Tuple[DataFrame, DataFrame]:
        if "cdc_operation" not in df.columns:
            return df, self.spark.createDataFrame([], df.schema.add("reject_rule", "string").add("reject_reason", "string"))
        
        valid_df = df.filter(F.col("cdc_operation") != "D")
        rejects_df = df.filter(F.col("cdc_operation") == "D").withColumn("reject_rule", F.lit("cdc_operation")) \
                                                         .withColumn("reject_reason", F.lit("CDC operation is Delete (soft delete)"))
        
        return valid_df, rejects_df

    def _check_enums(
        self,
        df: DataFrame,
        table: TableConfig,
        execution_id: str,
        reference_date: str
    ) -> Tuple[DataFrame, DataFrame]:
        valid_df = df
        all_rejects = []

        for col_name, allowed_values in table.enum_columns.items():
            if col_name not in df.columns:
                logger.warning(f"Enum column {col_name} not found in DataFrame for {table.target_table}")
                continue
            
            allowed_list = ", ".join([f"'{v}'" for v in allowed_values])
            valid_condition = F.col(col_name).isin(allowed_values)
            
            rejects = valid_df.filter(~valid_condition).withColumn("reject_rule", F.lit("enum_validation")) \
                                                          .withColumn("reject_reason", F.lit(f"Value not in allowed list: [{allowed_list}]"))
            
            valid_df = valid_df.filter(valid_condition)
            all_rejects.append(rejects)

        if all_rejects:
            from functools import reduce
            rejects_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_rejects)
        else:
            rejects_df = self.spark.createDataFrame([], df.schema.add("reject_rule", "string").add("reject_reason", "string"))

        return valid_df, rejects_df

    def _check_fk_integrity(
        self,
        df: DataFrame,
        table: TableConfig,
        execution_id: str,
        reference_date: str
    ) -> Tuple[DataFrame, DataFrame]:
        fk_cols = [c for c in df.columns if c.endswith("_key")]
        
        if not fk_cols:
            return df, self.spark.createDataFrame([], df.schema.add("reject_rule", "string").add("reject_reason", "string"))

        valid_df = df
        all_rejects = []

        for fk_col in fk_cols:
            dim_name = fk_col.replace("_key", "")
            dim_table = f"{self.trusted_db}.dim_{dim_name}"
            
            try:
                dim_df = self.spark.table(dim_table).select(fk_col).distinct()
                valid_fks = dim_df.rdd.map(lambda r: r[0]).collect()
                if not valid_fks:
                    invalid = valid_df.filter(F.col(fk_col).isNotNull())
                else:
                    invalid = valid_df.filter(F.col(fk_col).isNotNull() & ~F.col(fk_col).isin(valid_fks))
                
                if invalid.count() > 0:
                    rejects = invalid.withColumn("reject_rule", F.lit("fk_integrity")) \
                                     .withColumn("reject_reason", F.lit(f"FK {fk_col} does not resolve in {dim_table}"))
                    all_rejects.append(rejects)
                    valid_df = valid_df.subtract(invalid)
                    
            except Exception as e:
                logger.warning(f"Could not validate FK {fk_col} against {dim_table}: {e}")

        if all_rejects:
            from functools import reduce
            rejects_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_rejects)
        else:
            rejects_df = self.spark.createDataFrame([], df.schema.add("reject_rule", "string").add("reject_reason", "string"))

        return valid_df, rejects_df