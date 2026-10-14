output "cluster_id" {
  description = "EMR Cluster ID"
  value       = aws_emr_cluster.main.id
}

output "workspace_bucket" {
  description = "Workspace bucket for artifacts and logs"
  value       = local.buckets.workspace
}

output "artifacts_bucket" {
  description = "Workspace/artifacts bucket"
  value       = local.buckets.workspace
}

output "kms_key_arn" {
  description = "KMS Key ARN"
  value       = aws_kms_key.main.arn
}