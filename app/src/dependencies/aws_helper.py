import boto3
import json
import logging
from typing import Any, Dict, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_account_id() -> str:
    """Get current AWS account ID."""
    sts = boto3.client("sts")
    return sts.get_caller_identity()["Account"]


def get_json_from_s3(s3_path: str) -> Dict[str, Any]:
    """Load JSON from S3 path."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got: {s3_path}")

    bucket = s3_path.split("/")[2]
    key = "/".join(s3_path.split("/")[3:])

    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except ClientError as e:
        logger.error(f"Failed to load JSON from {s3_path}: {e}")
        raise


def put_json_to_s3(data: Dict, s3_path: str):
    """Write JSON to S3 path."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got: {s3_path}")

    bucket = s3_path.split("/")[2]
    key = "/".join(s3_path.split("/")[3:])

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


def get_table_location(spark, database: str, table: str) -> Optional[str]:
    """Get S3 location of a Delta table from Glue Catalog."""
    try:
        desc = spark.sql(f"DESCRIBE FORMATTED {database}.{table}").collect()
        for row in desc:
            if row.col_name == "Location":
                return row.data_type
    except Exception:
        pass
    return None


def put_metric(
    namespace: str,
    metric_name: str,
    value: float,
    unit: str = "Count",
    dimensions: Optional[Dict[str, str]] = None
):
    """Put custom metric to CloudWatch."""
    cloudwatch = boto3.client("cloudwatch")
    
    metric_data = {
        "MetricName": metric_name,
        "Value": value,
        "Unit": unit
    }
    
    if dimensions:
        metric_data["Dimensions"] = [
            {"Name": k, "Value": v} for k, v in dimensions.items()
        ]
    
    cloudwatch.put_metric_data(Namespace=namespace, MetricData=[metric_data])


def put_log(log_group: str, log_stream: str, message: str):
    """Put log event to CloudWatch Logs."""
    logs = boto3.client("logs")
    
    try:
        logs.create_log_group(logGroupName=log_group)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    
    try:
        logs.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    
    logs.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=[{"timestamp": int(__import__("time").time() * 1000), "message": message}]
    )


def get_glue_database_location(database: str) -> Optional[str]:
    """Get S3 location of a Glue database."""
    glue = boto3.client("glue")
    try:
        response = glue.get_database(Name=database)
        return response["Database"].get("LocationUri")
    except ClientError:
        return None


def ensure_glue_database(spark, database: str, location: str):
    """Ensure Glue database exists."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{location}'")


def build_s3_path(bucket: str, *parts: str) -> str:
    """Build S3 path from bucket and parts."""
    return "s3://" + "/".join([bucket] + list(parts))