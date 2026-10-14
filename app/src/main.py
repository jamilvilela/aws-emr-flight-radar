#!/usr/bin/env python3
"""
EMR ETL - Bronze to Trusted layer processing.
Entry point for Spark job.
"""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession

from dependencies.config_models import TablesConfig
from dependencies.pipeline import PipelineRunner


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EMR ETL (Bronze -> Trusted)")
    parser.add_argument("--tables", required=True,
                        help="Lista de tabelas a processar (ex: airports,flights ou all)")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--processing_date", help="Data de processamento (YYYY-MM-DD)")
    parser.add_argument("--config_s3_path", required=True, help="S3 path do tables_config.json")
    parser.add_argument("--raw_database", default="db_raw", help="Database origem (Bronze)")
    parser.add_argument("--trusted_database", default="db_trusted", help="Database destino (Trusted)")
    parser.add_argument("--log_level", choices=["DEBUG", "INFO", "WARN"], default="INFO")
    return parser.parse_args()


def _init_spark(app_name: str = "emr-etl") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    builder = builder.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    builder = builder.config("spark.databricks.delta.optimizeWrite.enabled", "true")
    builder = builder.config("spark.databricks.delta.autoCompact.enabled", "true")
    
    builder = builder.config("spark.eventLog.enabled", "true")
    
    return builder.getOrCreate()


def _resolve_processing_date(args: argparse.Namespace) -> str:
    if args.mode == "incremental":
        if args.processing_date:
            return args.processing_date
        return datetime.utcnow().strftime("%Y-%m-%d")
    else:
        return args.processing_date or datetime.utcnow().strftime("%Y-%m-%d")


def main():
    args = _parse_args()
    
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting EMR ETL")
    logger.info(f"Args: tables={args.tables}, mode={args.mode}, processing_date={args.processing_date}")
    
    spark = _init_spark()
    spark.sparkContext.setLogLevel(args.log_level)
    
    try:
        logger.info(f"Loading config from {args.config_s3_path}")
        config = TablesConfig.from_s3(args.config_s3_path)
        
        processing_date = _resolve_processing_date(args)
        logger.info(f"Processing date: {processing_date}")
        
        runner = PipelineRunner(
            spark, config, args.raw_database, args.trusted_database,
            args.mode, processing_date
        )
        
        runner.run(args.tables)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()