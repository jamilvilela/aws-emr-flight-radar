data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  buckets = {
    raw       = var.raw_bucket != "" ? var.raw_bucket : "lakehouse-raw-${local.account_id}"
    trusted   = var.trusted_bucket != "" ? var.trusted_bucket : "lakehouse-trusted-${local.account_id}"
    workspace = var.checkpoint_bucket != "" ? var.checkpoint_bucket : "lakehouse-workspace-${local.account_id}"
  }

  # Pre-computed bucket ARNs for IAM policies (avoids interpolation issues in jsonencode)
  bucket_arns = {
    raw       = "arn:aws:s3:::${local.buckets.raw}"
    raw_wild  = "arn:aws:s3:::${local.buckets.raw}/*"
    trusted   = "arn:aws:s3:::${local.buckets.trusted}"
    trusted_wild = "arn:aws:s3:::${local.buckets.trusted}/*"
    workspace = "arn:aws:s3:::${local.buckets.workspace}"
    workspace_wild = "arn:aws:s3:::${local.buckets.workspace}/*"
    logs      = "arn:aws:s3:::${local.buckets.workspace}"
    logs_wild = "arn:aws:s3:::${local.buckets.workspace}/*"
  }

  script_location = "s3://${local.buckets.workspace}/aws-emr/flight-radar/jobs/main.py"
  config_s3_path  = "s3://${local.buckets.workspace}/aws-emr/flight-radar/config/tables_config.json"
  py_files        = "s3://${local.buckets.workspace}/aws-emr/flight-radar/dependencies/emr_helpers.zip"

  kms_key_alias = "alias/emr-flight-radar"

  # Conditionally include processing_date argument
  processing_date_args = var.processing_date != "" ? ["--processing_date", var.processing_date] : []

  spark_args = concat([
    "spark-submit", "--deploy-mode", "cluster", "--master", "yarn",
    "--packages", "io.delta:delta-spark_2.13:3.1.0",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "--conf", "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "--conf", "spark.sql.adaptive.enabled=true",
    "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
    "--conf", "spark.sql.shuffle.partitions=200",
    "--conf", "spark.executor.memory=8g",
    "--conf", "spark.executor.cores=4",
    "--conf", "spark.driver.memory=4g",
    "--conf", "spark.databricks.delta.optimizeWrite.enabled=true",
    "--conf", "spark.databricks.delta.autoCompact.enabled=true",
    "--conf", "spark.eventLog.enabled=true",
    "--conf", "spark.eventLog.dir=s3://${local.buckets.workspace}/aws-emr/flight-radar/spark-events/",
    "--conf", "spark.history.fs.logDirectory=s3://${local.buckets.workspace}/aws-emr/flight-radar/spark-events/",
    "--py-files", local.py_files,
    local.script_location,
    "--tables", var.processing_tables,
    "--mode", var.processing_mode,
  ], local.processing_date_args, [
    "--config_s3_path", local.config_s3_path,
    "--raw_database", var.raw_database,
    "--trusted_database", var.trusted_database,
    "--log_level", var.log_level,
  ])

  common_tags = merge({
    Project     = "flight-radar"
    Environment = "prod"
    ManagedBy   = "terraform"
  }, var.tags)
}