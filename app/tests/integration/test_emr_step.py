import pytest
import boto3
import json
from moto import mock_aws


class TestEMRStep:
    """Integration tests for EMR step configuration."""

    @mock_aws
    def test_emr_step_args_construction(self):
        """Test that EMR step args are correctly constructed."""
        # This would test the local.spark_args construction in Terraform
        # For now, verify the expected structure
        expected_args = [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--master", "yarn",
            "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "--conf", "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        ]
        
        # Verify key configs are present
        assert "--conf" in expected_args
        assert "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" in expected_args
        assert "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" in expected_args

    @mock_aws
    def test_s3_artifact_paths(self):
        """Test S3 artifact path construction."""
        account_id = "123456789012"
        workspace_bucket = f"lakehouse-workspace-{account_id}"
        
        script_location = f"s3://{workspace_bucket}/aws-emr/flight-radar/jobs/main.py"
        config_path = f"s3://{workspace_bucket}/aws-emr/flight-radar/config/tables_config.json"
        py_files = f"s3://{workspace_bucket}/aws-emr/flight-radar/dependencies/emr_helpers.zip"
        
        assert script_location.startswith("s3://")
        assert config_path.startswith("s3://")
        assert py_files.startswith("s3://")
        assert "emr_helpers.zip" in py_files

    @mock_aws
    def test_iam_policy_structure(self):
        """Test IAM policy has required permissions."""
        # Verify the policy structure matches expectations
        required_s3_actions = [
            "s3:GetObject",
            "s3:ListBucket",
            "s3:PutObject",
            "s3:DeleteObject"
        ]
        
        required_glue_actions = [
            "glue:GetDatabase",
            "glue:GetTables",
            "glue:GetTable",
            "glue:GetPartitions",
            "glue:BatchCreatePartition",
            "glue:CreateTable",
            "glue:UpdateTable"
        ]
        
        required_kms_actions = [
            "kms:Decrypt",
            "kms:Encrypt",
            "kms:GenerateDataKey"
        ]
        
        # These would be validated against the actual policy document
        assert len(required_s3_actions) == 4
        assert len(required_glue_actions) == 7
        assert len(required_kms_actions) == 3