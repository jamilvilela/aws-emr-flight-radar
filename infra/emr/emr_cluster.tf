resource "aws_emr_cluster" "main" {
  name                   = var.emr_cluster_config.name
  release_label          = var.emr_cluster_config.release_label
  applications           = ["Spark", "Livy", "Hadoop", "Hive"]
  service_role           = aws_iam_role.emr_service.arn
  security_configuration = aws_emr_security_configuration.main.id
  log_uri                = "s3://${local.buckets.workspace}/aws-emr/flight-radar/cluster-logs/"

  ec2_attributes {
    subnet_id                         = local.private_subnet_id
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
    name                 = "core"
    target_spot_capacity = var.emr_cluster_config.core_instance_count
    instance_type_configs {
      instance_type = var.emr_cluster_config.instance_type_core
      ebs_config {
        size                 = var.emr_cluster_config.core_ebs_size_gb
        type                 = "gp3"
        volumes_per_instance = 1
      }
      bid_price_as_percentage_of_on_demand_price = var.emr_cluster_config.core_spot_bid_price_pct
    }
  }

  auto_termination_policy {
    idle_timeout = var.emr_cluster_config.auto_termination_idle * 60
  }

  termination_protection = false
  visible_to_all_users   = true

  tags = local.common_tags

  bootstrap_action {
    name = "Install Delta Lake Jars"
    path = "s3://lakehouse-workspace-331504768406/aws-emr/flight-radar/bootstrap/install-delta-jars.sh"
    args = []
  }

  step {
    name              = "Silver ETL - ${var.processing_tables}"
    action_on_failure = "TERMINATE_CLUSTER"
    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = local.spark_args
    }
  }
}

resource "aws_emr_instance_fleet" "task" {
  count                   = var.emr_cluster_config.task_instance_count > 0 ? 1 : 0
  cluster_id              = aws_emr_cluster.main.id
  name                    = "task"
  target_spot_capacity    = var.emr_cluster_config.task_instance_count
  instance_type_configs {
    instance_type = var.emr_cluster_config.instance_type_task
    ebs_config {
      size                 = var.emr_cluster_config.task_ebs_size_gb
      type                 = "gp3"
      volumes_per_instance = 1
    }
    bid_price_as_percentage_of_on_demand_price = var.emr_cluster_config.core_spot_bid_price_pct
    weighted_capacity = 1
  }
}

resource "aws_emr_managed_scaling_policy" "main" {
  count      = var.emr_cluster_config.managed_scaling ? 1 : 0
  cluster_id = aws_emr_cluster.main.id
  compute_limits {
    unit_type              = "InstanceFleetUnits"
    minimum_capacity_units = var.emr_cluster_config.min_capacity
    maximum_capacity_units = var.emr_cluster_config.max_capacity
  }
}