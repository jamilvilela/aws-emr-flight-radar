# Service role (EMR)
resource "aws_iam_role" "emr_service" {
  name = "role-emr-service-flight-radar-prod"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

# EC2 role (nodes)
resource "aws_iam_role" "emr_ec2" {
  name = "role-emr-ec2-flight-radar-prod"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

# AWS managed policies for EMR roles
resource "aws_iam_role_policy_attachment" "emr_service_managed" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role_policy_attachment" "emr_ec2_managed" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Instance profile for EC2 role
resource "aws_iam_instance_profile" "emr_ec2" {
  name = "profile-emr-ec2-flight-radar-prod"
  role = aws_iam_role.emr_ec2.name
}

# Data access policy (least privilege)
resource "aws_iam_policy" "emr_data_access" {
  name = "policy-emr-flight-radar-data-access-prod"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          local.bucket_arns.raw,
          local.bucket_arns.raw_wild,
          local.bucket_arns.workspace,
          local.bucket_arns.workspace_wild,
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          local.bucket_arns.trusted,
          local.bucket_arns.trusted_wild,
          local.bucket_arns.workspace,
          local.bucket_arns.workspace_wild,
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
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.main.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLog*"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["sts:GetCallerIdentity"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_s3" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.emr_data_access.arn
}

# Attach to service role as well
resource "aws_iam_role_policy_attachment" "emr_service_s3" {
  role       = aws_iam_role.emr_service.name
  policy_arn = aws_iam_policy.emr_data_access.arn
}

# Lake Formation grants - managed externally (require Lake Formation admin)
# resource "aws_lakeformation_permissions" "raw_select" {
#   principal   = aws_iam_role.emr_service.arn
#   permissions = ["SELECT"]
#   database { name = var.raw_database }
# }
#
# resource "aws_lakeformation_permissions" "trusted_all" {
#   principal   = aws_iam_role.emr_service.arn
#   permissions = ["ALL"]
#   database { name = var.trusted_database }
# }