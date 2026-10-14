import pytest
import boto3
from moto import mock_aws
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="session")
def mock_s3(aws_credentials):
    """Mock S3 for testing."""
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="session")
def mock_glue(aws_credentials):
    """Mock Glue for testing."""
    with mock_aws():
        yield boto3.client("glue", region_name="us-east-1")


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for integration testing."""
    spark = SparkSession.builder \
        .appName("test-emr-silver-integration") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()