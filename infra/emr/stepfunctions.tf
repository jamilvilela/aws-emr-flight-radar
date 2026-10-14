# Step Functions EMR integration - commented out (integration not available in this region)
# Uncomment when EMR integration becomes available in this region

# resource "aws_iam_role" "stepfunctions_role" {
#   name = "role-stepfunctions-flight-radar-prod"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect    = "Allow"
#       Principal = { Service = "states.amazonaws.com" }
#       Action    = "sts:AssumeRole"
#     }]
#   })
#
#   tags = local.common_tags
# }
#
# resource "aws_iam_role_policy" "stepfunctions_policy" {
#   name = "policy-stepfunctions-flight-radar-prod"
#   role = aws_iam_role.stepfunctions_role.id
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "elasticmapreduce:RunJobFlow",
#           "elasticmapreduce:DescribeCluster",
#           "elasticmapreduce:DescribeStep",
#           "elasticmapreduce:AddJobFlowSteps",
#           "elasticmapreduce:TerminateJobFlows",
#           "elasticmapreduce:ListClusters",
#           "elasticmapreduce:ListSteps"
#         ]
#         Resource = "*"
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "iam:PassRole"
#         ]
#         Resource = [
#           aws_iam_role.emr_service.arn,
#           aws_iam_role.emr_ec2.arn
#         ]
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "events:PutTargets",
#           "events:PutRule",
#           "events:DescribeRule"
#         ]
#         Resource = "*"
#       }
#     ]
#   })
# }
#
# resource "aws_iam_role" "eventbridge_role" {
#   name = "role-eventbridge-flight-radar-prod"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect    = "Allow"
#       Principal = { Service = "events.amazonaws.com" }
#       Action    = "sts:AssumeRole"
#     }]
#   })
#
#   tags = local.common_tags
# }
#
# resource "aws_iam_role_policy" "eventbridge_policy" {
#   name = "policy-eventbridge-flight-radar-prod"
#   role = aws_iam_role.eventbridge_role.id
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect   = "Allow"
#       Action   = "states:StartExecution"
#       Resource = aws_sfn_state_machine.etl.arn
#     }]
#   })
# }
#
# resource "aws_sfn_state_machine" "etl" {
#   name     = "run-etl-prod"
#   role_arn = aws_iam_role.stepfunctions_role.arn
#   definition = jsonencode({
#     Comment = "EMR ETL Orchestration"
#     StartAt = "RunJobFlow"
#     States = {
#       RunJobFlow = {
#         Type     = "Task"
#         Resource = "arn:aws:states:::emr:createCluster.sync"
#         Parameters = {
#           Name         = var.emr_cluster_config.name
#           ReleaseLabel = var.emr_cluster_config.release_label
#           Applications = [{ Name = "Spark" }, { Name = "Hive" }, { Name = "Livy" }]
#           ServiceRole  = aws_iam_role.emr_service.arn
#           JobFlowRole  = aws_iam_instance_profile.emr_ec2.arn
#           LogUri       = "s3://${local.buckets.logs}/emr/cluster-logs/"
#           Instances = {
#             InstanceFleets = [
#               {
#                 Name                   = "MASTER"
#                 InstanceFleetType      = "MASTER"
#                 TargetOnDemandCapacity = 1
#                 InstanceTypeConfigs = [{
#                   InstanceType = var.emr_cluster_config.instance_type_master
#                   EbsConfiguration = {
#                     EbsBlockDeviceConfigs = [{
#                       VolumeSpecification = {
#                         SizeInGB   = var.emr_cluster_config.core_ebs_size_gb
#                         VolumeType = "gp3"
#                       }
#                       VolumesPerInstance = 1
#                     }]
#                   }
#                 }]
#               },
#               {
#                 Name                   = "CORE"
#                 InstanceFleetType      = "CORE"
#                 TargetOnDemandCapacity = var.emr_cluster_config.core_instance_count
#                 InstanceTypeConfigs = [{
#                   InstanceType = var.emr_cluster_config.instance_type_core
#                   EbsConfiguration = {
#                     EbsBlockDeviceConfigs = [{
#                       VolumeSpecification = {
#                         SizeInGB   = var.emr_cluster_config.core_ebs_size_gb
#                         VolumeType = "gp3"
#                       }
#                       VolumesPerInstance = 1
#                     }]
#                   }
#                 }]
#               }
#             ]
#             Ec2SubnetId                    = local.private_subnet_id
#             EmrManagedMasterSecurityGroup  = aws_security_group.master.id
#             EmrManagedSlaveSecurityGroup   = aws_security_group.core.id
#             EmrManagedServiceSecurityGroup = aws_security_group.service.id
#           }
#           AutoTerminationPolicy = {
#             IdleTimeout = var.emr_cluster_config.auto_termination_idle * 60
#           }
#           Steps = [{
#             Name            = "ETL"
#             ActionOnFailure = "TERMINATE_CLUSTER"
#             HadoopJarStep = {
#               Jar  = "command-runner.jar"
#               Args = local.spark_args
#             }
#           }]
#           ManagedScalingPolicy = {
#             ComputeLimits = {
#               UnitType             = "Instances"
#               MinimumCapacityUnits = var.emr_cluster_config.min_capacity
#               MaximumCapacityUnits = var.emr_cluster_config.max_capacity
#             }
#           }
#           SecurityConfiguration = aws_emr_security_configuration.main.id
#         }
#         Next = "Terminate"
#         Retry = [{
#           ErrorEquals     = ["States.ALL"]
#           IntervalSeconds = 60
#           MaxAttempts     = 2
#           BackoffRate     = 2.0
#         }]
#       }
#       Terminate = {
#         Type       = "Task"
#         Resource   = "arn:aws:states:::emr:terminateJobFlow.sync"
#         Parameters = jsondecode("{\"JobFlowId.$\": \"$.ClusterId\"}")
#         End        = true
#       }
#     }
#   })
#
#   tags = local.common_tags
# }
#
# resource "aws_cloudwatch_event_rule" "hourly" {
#   name                = "emr-etl-hourly-prod"
#   schedule_expression = "rate(1 hour)"
#   state               = "ENABLED"
#   description         = "Hourly trigger for ETL"
#
#   tags = local.common_tags
# }
#
# resource "aws_cloudwatch_event_target" "sfn" {
#   rule      = aws_cloudwatch_event_rule.hourly.name
#   target_id = "sfn"
#   arn       = aws_sfn_state_machine.etl.arn
#   role_arn  = aws_iam_role.eventbridge_role.arn
#   input = jsonencode({
#     processing_date = ""
#     tables          = "all"
#     mode            = "incremental"
#   })
# }