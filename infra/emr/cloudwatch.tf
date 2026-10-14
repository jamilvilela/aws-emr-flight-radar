resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr/${var.emr_cluster_config.name}"
  retention_in_days = 30

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "step_failed" {
  alarm_name          = "emr-etl-step-failed-prod"
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
  alarm_name          = "emr-etl-step-duration-prod"
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
  alarm_name          = "emr-etl-quality-threshold-prod"
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