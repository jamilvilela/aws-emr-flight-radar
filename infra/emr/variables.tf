variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "emr_cluster_config" {
  description = "EMR cluster configuration"
  type = object({
    name                        = string
    release_label               = string
    applications                = list(string)
    instance_type_master        = string
    instance_type_core          = string
    instance_type_task          = string
    core_instance_count         = number
    task_instance_count         = number
    core_ebs_size_gb            = number
    task_ebs_size_gb            = number
    core_spot_bid_price_pct     = number
    auto_termination_idle       = number
    managed_scaling             = bool
    min_capacity                = number
    max_capacity                = number
  })
  default = {
    name                        = "flight-radar-etl"
    release_label               = "emr-7.5.0"
    applications                = ["Spark", "Hive"]
    instance_type_master        = "m6g.xlarge"
    instance_type_core          = "r6g.2xlarge"
    instance_type_task          = "c6g.2xlarge"
    core_instance_count         = 2
    task_instance_count         = 1
    core_ebs_size_gb            = 100
    task_ebs_size_gb            = 200
    core_spot_bid_price_pct     = 90
    auto_termination_idle       = 30
    managed_scaling             = true
    min_capacity                = 1
    max_capacity                = 4
  }
}

variable "processing_tables" {
  description = "Tables to process (comma-separated or 'all')"
  type        = string
  default     = "all"
}

variable "processing_mode" {
  description = "Processing mode: full or incremental"
  type        = string
  default     = "incremental"
}

variable "processing_date" {
  description = "Processing date (YYYY-MM-DD)"
  type        = string
  default     = ""
}

variable "raw_database" {
  description = "Raw database name"
  type        = string
  default     = "db_raw"
}

variable "trusted_database" {
  description = "Trusted database name"
  type        = string
  default     = "db_trusted"
}

variable "raw_bucket" {
  description = "Raw bucket name"
  type        = string
  default     = ""
}

variable "trusted_bucket" {
  description = "Trusted bucket name"
  type        = string
  default     = ""
}

variable "checkpoint_bucket" {
  description = "Checkpoint/workspace bucket name"
  type        = string
  default     = ""
}

variable "log_level" {
  description = "Log level"
  type        = string
  default     = "INFO"
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}