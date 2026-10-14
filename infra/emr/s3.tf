# S3 buckets already exist - using data sources to reference them
# Bucket creation is handled separately

resource "aws_s3_object" "main_py" {
  bucket = local.buckets.workspace
  key    = "aws-emr/flight-radar/jobs/main.py"
  source = "${path.module}/../../app/src/main.py"
  etag   = filemd5("${path.module}/../../app/src/main.py")
}

resource "aws_s3_object" "config_json" {
  bucket = local.buckets.workspace
  key    = "aws-emr/flight-radar/config/tables_config.json"
  source = "${path.module}/../../app/src/dependencies/config/tables_config.json"
  etag   = filemd5("${path.module}/../../app/src/dependencies/config/tables_config.json")
}

resource "aws_s3_object" "sql_files" {
  for_each = fileset("${path.module}/../../app/src/dependencies/sql", "*.sql")
  bucket   = local.buckets.workspace
  key      = "aws-emr/flight-radar/dependencies/sql/${each.value}"
  source   = "${path.module}/../../app/src/dependencies/sql/${each.value}"
  etag     = filemd5("${path.module}/../../app/src/dependencies/sql/${each.value}")
}

resource "aws_s3_object" "py_files" {
  for_each = fileset("${path.module}/../../app/src/dependencies", "*.py")
  bucket   = local.buckets.workspace
  key      = "aws-emr/flight-radar/dependencies/${each.value}"
  source   = "${path.module}/../../app/src/dependencies/${each.value}"
  etag     = filemd5("${path.module}/../../app/src/dependencies/${each.value}")
}

resource "aws_s3_object" "jobs_files" {
  for_each = fileset("${path.module}/../../app/src/jobs", "*.py")
  bucket   = local.buckets.workspace
  key      = "aws-emr/flight-radar/dependencies/jobs/${each.value}"
  source   = "${path.module}/../../app/src/jobs/${each.value}"
  etag     = filemd5("${path.module}/../../app/src/jobs/${each.value}")
}