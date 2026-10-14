---
id: emr-terraform-spec
title: Spec Infra — Terraform EMR Trusted (infra/emr)
status: draft
version: 1.0
created: 2026-09-14
updated: 2026-09-14
author: Data Engineering Team
---

# Spec — Terraform Module `infra/emr`

Módulo Terraform do cluster **EMR under demand**. Reutiliza o VPC e padrões de nome do módulo
Glue (`infra/`): buckets `lakehouse-{tier}-{account_id}`, roles separadas, catálogo não-gerenciado
(`db_raw`/`db_trusted` existem no Data Lake).

## 1. Structure de arquivos

```
infra/emr/
├── main.tf
├── emr_cluster.tf
├── emr_security.tf
├── iam.tf
├── s3.tf
├── vpc.tf
├── cloudwatch.tf
├── stepfunctions.tf
├── variables.tf
├── outputs.tf
├── locals.tf
├── versions.tf
└── terraform.tfvars
```

## 2. `locals.tf`

```hcl
locals {
  account_id = data.aws_caller_identity.current.account_id

  buckets = {
    raw        = var.raw_bucket != "" ? var.raw_bucket : "lakehouse-raw-${local.account_id}"
    trusted    = var.trusted_bucket != "" ? var.trusted_bucket : "lakehouse-trusted-${local.account_id}"
    workspace  = var.checkpoint_bucket != "" ? var.checkpoint_bucket : "lakehouse-workspace-${local.account_id}"
    logs       = "lakehouse-logs-${local.account_id}"
  }

  script_location = "s3://${local.buckets.workspace}/aws-emr/flight-radar/jobs/main.py"
  config_s3_path  = "s3://${local.buckets.workspace}/aws-emr/flight-radar/config/tables_config.json"
  py_files        = "s3://${local.buckets.workspace}/aws-emr/flight-radar/dependencies/emr_helpers.zip"

  kms_key_alias = "alias/emr-flight-radar"

  spark_args = [
    "spark-submit", "--deploy-mode", "cluster", "--master", "yarn",
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
    "--conf", "spark.eventLog.dir=s3://${local.buckets.logs}/emr/spark-events/",
    "--conf", "spark.history.fs.logDirectory=s3://${local.buckets.logs}/emr/spark-events/",
    "--py-files", local.py_files,
    local.script_location,
    "--tables", var.processing_tables,
    "--mode", var.processing_mode,
    "--processing_date", var.processing_date,
    "--config_s3_path", local.config_s3_path,
    "--raw_database", var.raw_database,
    "--trusted_database", var.trusted_database,
    "--log_level", var.log_level,
  ]
}
```

## 3. `variables.tf` (cluster parametrizado)

```hcl
variable "emr_cluster_config" {
  type = object({
    name                  = string
    release_label         = string
    applications          = list(string)
    instance_type_master  = string
    instance_type_core    = string
    instance_type_task    = string
    core_instance_count   = number
    task_instance_count   = number
    core_ebs_size_gb      = number
    task_ebs_size_gb      = number
    auto_termination_idle = number
    managed_scaling       = bool
    min_capacity          = number
    max_capacity          = number
  })
  default = {
    name                  = "flight-radar-etl"
    release_label         = "emr-7.5.0"
    applications          = ["Spark", "Hive"]
    instance_type_master  = "m6g.xlarge"
    instance_type_core    = "m6g.2xlarge"
    instance_type_task    = "m6g.2xlarge"
    core_instance_count   = 3
    task_instance_count   = 0
    core_ebs_size_gb      = 100
    task_ebs_size_gb      = 200
    auto_termination_idle = 30
    managed_scaling       = true
    min_capacity          = 2
    max_capacity          = 10
  }
}

variable "processing_tables" { default = "all" }
variable "processing_mode" { default = "incremental" }
variable "processing_date" { default = "" }
variable "raw_database" { default = "db_raw" }
variable "trusted_database" { default = "db_trusted" }
variable "raw_bucket" { default = "" }
variable "trusted_bucket" { default = "" }
variable "checkpoint_bucket" { default = "" }
variable "log_level" { default = "INFO" }
variable "environment" { default = "dev" }
variable "tags" { default = {} }
```

## 4. `emr_cluster.tf`

```hcl
resource "aws_emr_cluster" "main" {
  name               = var.emr_cluster_config.name
  release_label      = var.emr_cluster_config.release_label
  applications       = ["Spark", "Livy", "Hadoop", "Hive"]
  service_role       = aws_iam_role.emr_service.arn
  security_configuration = aws_emr_security_configuration.main.id
  log_uri            = "s3://${aws_s3_bucket.logs.bucket}/emr/cluster-logs/"

  ec2_attributes {
    subnet_id                         = data.aws_subnet.private.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    emr_managed_master_security_group = aws_security_group.master.id
    emr_managed_slave_security_group  = aws_security_group.core.id
  }

  master_instance_fleet {
    name                      = "master"
    target_on_demand_capacity = 1
    instance_type_configs {
      instance_type = var.emr_cluster_config.instance_type_master
      ebs_config {
        size                 = var.emr_cluster_config.core_ebs_size_gb
        type                 = "gp3"
        volumes_per_instance = 1
      }
    }
  }

  core_instance_fleet {
    name                      = "core"
    target_on_demand_capacity = var.emr_cluster_config.core_instance_count
    instance_type_configs {
      instance_type = var.emr_cluster_config.instance_type_core
      ebs_config {
        size                 = var.emr_cluster_config.core_ebs_size_gb
        type                 = "gp3"
        volumes_per_instance = 1
      }
    }
  }

  auto_termination_policy {
    idle_timeout = var.emr_cluster_config.auto_termination_idle * 60
  }

  termination_protection = false
  visible_to_all_users   = true

  step {
    name           = "ETL - ${var.processing_tables}"
    action_on_failure = "TERMINATE_CLUSTER"
    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = local.spark_args
    }
  }

  tags = local.common_tags
}

resource "aws_emr_managed_scaling_policy" "main" {
  count      = var.emr_cluster_config.managed_scaling ? 1 : 0
  cluster_id = aws_emr_cluster.main.id
  compute_limits {
    unit_type              = "Instances"
    minimum_capacity_units = var.emr_cluster_config.min_capacity
    maximum_capacity_units = var.emr_cluster_config.max_capacity
  }
}
```

## 5. `emr_security.tf`

```hcl
resource "aws_kms_key" "main" {
  description             = "EMR flight-radar encryption"
  enable_key_rotation      = true
  deletion_window_in_days  = 30
}

resource "aws_kms_alias" "main" {
  name          = local.kms_key_alias
  target_key_id = aws_kms_key.main.key_id
}

resource "aws_emr_security_configuration" "main" {
  name = "emr-flight-radar-security"
  configuration = jsonencode({
    EncryptionConfiguration = {
      EnableInTransitEncryption           = false
      EnableAtRestEncryption              = true
      S3EncryptionConfiguration            = { EncryptionMode = "SSE-S3" }
      LocalDiskEncryptionConfiguration     = {
        EnableEbsEncryption = true
        EncryptionKeyProviderType = "AwsKms"
        AwsKmsKey = aws_kms_key.main.arn
      }
    }
  })
}
```

## 6. `iam.tf`

```hcl
# Service role (EMR)
resource "aws_iam_role" "emr_service" {
  name = "role-emr-service-flight-radar-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"]
}

# EC2 role (nodes)
resource "aws_iam_role" "emr_ec2" {
  name = "role-emr-ec2-flight-radar-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action  = "sts:AssumeRole"
    }]
  })
  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"]
}

# Instance profile for EC2 role
resource "aws_iam_instance_profile" "emr_ec2" {
  name = "profile-emr-ec2-flight-radar-${var.environment}"
  role = aws_iam_role.emr_ec2.name
}

# Data access policy (least privilege)
resource "aws_iam_policy" "emr_data_access" {
  name = "policy-emr-flight-radar-data-access-${var.environment}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${local.buckets.raw}",
          "arn:aws:s3:::${local.buckets.raw}/*",
          "arn:aws:s3:::${local.buckets.workspace}",
          "arn:aws:s3:::${local.buckets.workspace}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${local.buckets.trusted}",
          "arn:aws:s3:::${local.buckets.trusted}/*",
          "arn:aws:s3:::${local.buckets.logs}",
          "arn:aws:s3:::${local.buckets.logs}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTables",
          "glue:GetTable",
          "glue:GetPartitions",
          "glue:GetPartition",
          "glue:BatchCreatePartition",
          "glue:CreateTable",
          "glue:UpdateTable"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:database/${var.raw_database}",
          "arn:aws:glue:${var.region}:${local.account_id}:database/${var.trusted_database}",
          "arn:aws:glue:${var.region}:${local.account_id}:table/${var.raw_database}/*",
          "arn:aws:glue:${var.region}:${local.account_id}:table/${var.trusted_database}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.main.arn]
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLog*"]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = ["sts:GetCallerIdentity"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_s3" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.emr_data_access.arn
}

resource "aws_iam_role_policy_attachment" "emr_service_s3" {
  role       = aws_iam_role.emr_service.name
  policy_arn = aws_iam_policy.emr_data_access.arn
}

# Lake Formation grants
resource "aws_lakeformation_permissions" "raw_select" {
  principal     = aws_iam_role.emr_service.arn
  permissions = ["SELECT"]
  database { name = var.raw_database }
}

resource "aws_lakeformation_permissions" "trusted_all" {
  principal     = aws_iam_role.emr_service.arn
  permissions   = ["ALL"]
  database { name = var.trusted_database }
}
```

## 7. `s3.tf`

```hcl
resource "aws_s3_bucket" "artifacts" {
  bucket = local.buckets.workspace

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  versioning {
    enabled = true
  }
}

resource "aws_s3_bucket" "logs" {
  bucket = local.buckets.logs

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  lifecycle_rule {
    id      = "expire-logs"
    enabled = true
    expiration {
      days = 90
    }
  }
}

resource "aws_s3_object" "main_py" {
  bucket = aws_s3_bucket.artifacts.bucket
  key    = "aws-emr/flight-radar/jobs/main.py"
  source = "${path.module}/../../app/src/main.py"
  etag   = filemd5("${path.module}/../../app/src/main.py")
}

resource "aws_s3_object" "config_json" {
  bucket = aws_s3_bucket.artifacts.bucket
  key    = "aws-emr/flight-radar/config/tables_config.json"
  source = "${path.module}/../../app/src/dependencies/config/tables_config.json"
  etag   = filemd5("${path.module}/../../app/src/dependencies/config/tables_config.json")
}

resource "aws_s3_object" "sql_files" {
  for_each = fileset("${path.module}/../../app/src/dependencies/sql", "*.sql")
  bucket   = aws_s3_bucket.artifacts.bucket
  key      = "aws-emr/flight-radar/dependencies/sql/${each.value}"
  source   = "${path.module}/../../app/src/dependencies/sql/${each.value}"
  etag     = filemd5("${path.module}/../../app/src/dependencies/sql/${each.value}")
}

resource "aws_s3_object" "py_files" {
  for_each = fileset("${path.module}/../../app/src/dependencies", "*.py")
  bucket   = aws_s3_bucket.artifacts.bucket
  key      = "aws-emr/flight-radar/dependencies/${each.value}"
  source   = "${path.module}/../../app/src/dependencies/${each.value}"
  etag     = filemd5("${path.module}/../../app/src/dependencies/${each.value}")
}

resource "aws_s3_object" "jobs_files" {
  for_each = fileset("${path.module}/../../app/src/jobs", "*.py")
  bucket   = aws_s3_bucket.artifacts.bucket
  key      = "aws-emr/flight-radar/dependencies/jobs/${each.value}"
  source   = "${path.module}/../../app/src/jobs/${each.value}"
  etag     = filemd5("${path.module}/../../app/src/jobs/${each.value}")
}
```

## 8. `vpc.tf`

```hcl
data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "tag:Name"
    values = ["private*"]
  }
}

data "aws_security_group" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "group-name"
    values = ["default"]
  }
}

resource "aws_security_group" "master" {
  name        = "sg-emr-master-flight-radar-${var.environment}"
  description = "EMR Master security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

resource "aws_security_group" "core" {
  name        = "sg-emr-core-flight-radar-${var.environment}"
  description = "EMR Core/Task security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

resource "aws_security_group" "service" {
  name        = "sg-emr-service-flight-radar-${var.environment}"
  description = "EMR Service security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# Ingress rules (separate to avoid cycles)
resource "aws_security_group_rule" "master_ingress_core" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.master.id
  source_security_group_id = aws_security_group.core.id
}

resource "aws_security_group_rule" "master_ingress_service" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.master.id
  source_security_group_id = aws_security_group.service.id
}

resource "aws_security_group_rule" "core_ingress_master" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.core.id
  source_security_group_id = aws_security_group.master.id
}

resource "aws_security_group_rule" "core_ingress_self" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.core.id
  source_security_group_id = aws_security_group.core.id
}

resource "aws_security_group_rule" "service_ingress_master" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.service.id
  source_security_group_id = aws_security_group.master.id
}
```

## 9. `cloudwatch.tf`

```hcl
resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr/${var.emr_cluster_config.name}"
  retention_in_days = 30

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "step_failed" {
  alarm_name          = "emr-etl-step-failed-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  metric_name         = "ETLFailed"
  namespace           = "FlightRadar"
  period              = 300
  statistic           = "Sum"
  alarm_description   = "EMR ETL step failed"

  alarm_actions = []

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "step_duration" {
  alarm_name          = "emr-etl-step-duration-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 1200
  metric_name         = "ETL.Duration"
  namespace           = "FlightRadar"
  period              = 300
  statistic           = "Maximum"
  alarm_description   = "EMR ETL step duration exceeded 20 minutes"

  alarm_actions = []

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "quality_threshold" {
  alarm_name          = "emr-etl-quality-threshold-${var.environment}"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  threshold           = 0.95
  metric_name         = "ETL.QualityRatio"
  namespace           = "FlightRadar"
  period              = 300
  statistic           = "Minimum"
  alarm_description   = "EMR ETL quality ratio below 95%"

  alarm_actions = []

  tags = local.common_tags
}
```

## 10. `stepfunctions.tf`

```hcl
resource "aws_iam_role" "stepfunctions_role" {
  name = "role-stepfunctions-flight-radar-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "stepfunctions_policy" {
  name = "policy-stepfunctions-flight-radar-${var.environment}"
  role = aws_iam_role.stepfunctions_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticmapreduce:RunJobFlow",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:DescribeStep",
          "elasticmapreduce:AddJobFlowSteps",
          "elasticmapreduce:TerminateJobFlows",
          "elasticmapreduce:ListClusters",
          "elasticmapreduce:ListSteps"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.emr_service.arn,
          aws_iam_role.emr_ec2.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "eventbridge_role" {
  name = "role-eventbridge-flight-radar-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "policy-eventbridge-flight-radar-${var.environment}"
  role = aws_iam_role.eventbridge_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "states:StartExecution"
      Resource = aws_sfn_state_machine.etl.arn
    }]
  })
}

resource "aws_sfn_state_machine" "etl" {
  name     = "run-etl-${var.environment}"
  role_arn = aws_iam_role.stepfunctions_role.arn
  definition = jsonencode({
    Comment = "EMR ETL Orchestration"
    StartAt = "RunJobFlow"
    States = {
      RunJobFlow = {
        Type = "Task"
        Resource = "arn:aws:states:::emr:createCluster.sync"
        Parameters = {
          Name = var.emr_cluster_config.name
          ReleaseLabel = var.emr_cluster_config.release_label
          Applications = [{ Name = "Spark" }, { Name = "Hive" }, { Name = "Livy" }]
          ServiceRole = aws_iam_role.emr_service.arn
          JobFlowRole = aws_iam_instance_profile.emr_ec2.arn
          LogUri = "s3://${local.buckets.logs}/emr/cluster-logs/"
          Instances = {
            InstanceFleets = [
              {
                Name = "MASTER"
                InstanceFleetType = "MASTER"
                TargetOnDemandCapacity = 1
                InstanceTypeConfigs = [{
                  InstanceType = var.emr_cluster_config.instance_type_master
                  EbsConfiguration = {
                    EbsBlockDeviceConfigs = [{
                      VolumeSpecification = {
                        SizeInGB = var.emr_cluster_config.core_ebs_size_gb
                        VolumeType = "gp3"
                      }
                      VolumesPerInstance = 1
                    }]
                  }
                }]
              },
              {
                Name = "CORE"
                InstanceFleetType = "CORE"
                TargetOnDemandCapacity = var.emr_cluster_config.core_instance_count
                InstanceTypeConfigs = [{
                  InstanceType = var.emr_cluster_config.instance_type_core
                  EbsConfiguration = {
                    EbsBlockDeviceConfigs = [{
                      VolumeSpecification = {
                        SizeInGB = var.emr_cluster_config.core_ebs_size_gb
                        VolumeType = "gp3"
                      }
                      VolumesPerInstance = 1
                    }]
                  }
                }]
              }
            ]
            Ec2SubnetId = data.aws_subnet.private.id
            EmrManagedMasterSecurityGroup = aws_security_group.master.id
            EmrManagedSlaveSecurityGroup = aws_security_group.core.id
            EmrManagedServiceSecurityGroup = aws_security_group.service.id
          }
          AutoTerminationPolicy = {
            IdleTimeout = var.emr_cluster_config.auto_termination_idle * 60
          }
          Steps = [{
            Name = "ETL"
            ActionOnFailure = "TERMINATE_CLUSTER"
            HadoopJarStep = {
              Jar = "command-runner.jar"
              Args = local.spark_args
            }
          }]
          ManagedScalingPolicy = {
            ComputeLimits = {
              UnitType = "Instances"
              MinimumCapacityUnits = var.emr_cluster_config.min_capacity
              MaximumCapacityUnits = var.emr_cluster_config.max_capacity
            }
          }
          SecurityConfiguration = aws_emr_security_configuration.main.id
        }
        Next = "Terminate"
        Retry = [{
          ErrorEquals = ["States.ALL"]
          IntervalSeconds = 60
          MaxAttempts = 2
          BackoffRate = 2.0
        }]
      }
      Terminate = {
        Type = "Task"
        Resource = "arn:aws:states:::emr:terminateJobFlow.sync"
        Parameters = jsondecode("{\"JobFlowId.$\": \"$.ClusterId\"}")
        End = true
      }
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_rule" "hourly" {
  name                = "emr-etl-hourly-${var.environment}"
  schedule_expression = "rate(1 hour)"
  state               = "ENABLED"
  description         = "Hourly trigger for ETL"

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "sfn" {
  rule      = aws_cloudwatch_event_rule.hourly.name
  target_id = "sfn"
  arn       = aws_sfn_state_machine.etl.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
  input     = jsonencode({
    processing_date = ""
    tables          = "all"
    mode            = "incremental"
  })
}
```

## 11. `outputs.tf`

```hcl
output "cluster_id" {
  description = "EMR Cluster ID"
  value       = aws_emr_cluster.main.id
}

output "logs_bucket" {
  description = "S3 bucket for logs"
  value       = aws_s3_bucket.logs.bucket
}

output "state_machine_arn" {
  description = "Step Functions State Machine ARN"
  value       = aws_sfn_state_machine.etl.arn
}

output "artifacts_bucket" {
  description = "Workspace/artifacts bucket"
  value       = aws_s3_bucket.artifacts.bucket
}

output "kms_key_arn" {
  description = "KMS Key ARN"
  value       = aws_kms_key.main.arn
}
```

## 12. Notas

- O exemplo é **didático**; em produção, use a sintaxe exata do provider `hashicorp-aws` atual
  (ex.: `auto_termination` vs `auto_termination_policy`, `instance_fleet` vs `instance_group`).
- O módulo assume que `db_trusted` e `db_raw` já existem e que buckets são criados fora do módulo
  (ou via `aws_s3_bucket` c/ `force_destroy` desativado).
- Use `local.spark_args` para injetar os parâmetros da aplicação.
- **Sem parâmetros de bucket** no spark-submit — o Writer usa `DeltaTable.forName()` via Glue Catalog.

---

**Fim do documento.** Salve como `emr_terraform_spec.md`.