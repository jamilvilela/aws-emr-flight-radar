#!/usr/bin/env bash
# Rollback EMR ETL infrastructure (destroy Terraform resources)
# Data in S3 buckets (Trusted tables, logs) is NOT removed.
# Usage: ./rollback-emr.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="$PROJECT_ROOT/infra/emr"
TFVARS_FILE="$TERRAFORM_DIR/terraform.tfvars"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

log_warn "============================================"
log_warn "ROLLBACK: This will DESTROY EMR infrastructure (prod)"
log_warn "============================================"
log_warn "The following will be REMOVED:"
log_warn "  - EMR Cluster"
log_warn "  - IAM Roles and Policies"
log_warn "  - Security Groups"
log_warn "  - KMS Key"
log_warn "  - CloudWatch Log Groups and Alarms"
log_warn "  - Step Functions State Machine"
log_warn "  - EventBridge Rules"
log_warn ""
log_warn "The following will NOT be removed:"
log_warn "  - S3 Buckets (raw, trusted, workspace, logs)"
log_warn "  - Delta Tables data (db_trusted)"
log_warn "  - Glue Catalog databases (db_raw, db_trusted)"
log_warn "============================================"

read -p "Are you sure you want to continue? (type 'yes' to confirm): " CONFIRM
if [[ "$CONFIRM" != "yes" ]]; then
    log_info "Rollback cancelled"
    exit 0
fi

# Check prerequisites
command -v terraform >/dev/null 2>&1 || { log_error "terraform not found in PATH"; exit 1; }

if [[ ! -f "$TFVARS_FILE" ]]; then
    log_error "terraform.tfvars not found at $TFVARS_FILE"
    exit 1
fi

cd "$TERRAFORM_DIR"

log_info "Initializing Terraform..."
terraform init -upgrade

log_info "Running terraform destroy..."
terraform destroy -var-file="$TFVARS_FILE" -auto-approve

log_info "============================================"
log_info "Rollback completed successfully"
log_info "============================================"
log_info "S3 buckets and data preserved:"
log_info "  - lakehouse-raw-{account_id}"
log_info "  - lakehouse-trusted-{account_id}"
log_info "  - lakehouse-workspace-{account_id}"
log_info "  - lakehouse-logs-{account_id}"
log_info ""
log_info "To fully clean up, manually delete S3 buckets and Glue databases if needed."