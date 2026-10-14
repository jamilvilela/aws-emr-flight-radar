#!/usr/bin/env bash
# Deploy EMR ETL infrastructure and upload artifacts
# Usage: ./deploy-emr.sh [plan|apply]

set -euo pipefail

ACTION="${1:-apply}"
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

# Validate action
if [[ ! "$ACTION" =~ ^(plan|apply)$ ]]; then
    log_error "Invalid action: $ACTION. Use 'plan' or 'apply'"
    exit 1
fi

log_info "Deploying EMR ETL (prod environment)"
log_info "Action: $ACTION"

# Check prerequisites
command -v terraform >/dev/null 2>&1 || { log_error "terraform not found in PATH"; exit 1; }
command -v aws >/dev/null 2>&1 || { log_error "aws cli not found in PATH"; exit 1; }
command -v python3 >/dev/null 2>&1 || { log_error "python3 not found in PATH"; exit 1; }

# Check tfvars file exists
if [[ ! -f "$TFVARS_FILE" ]]; then
    log_warn "terraform.tfvars not found, copying from example"
    cp "$TERRAFORM_DIR/terraform.tfvars.example" "$TFVARS_FILE"
    log_warn "Please edit $TFVARS_FILE with your environment values"
    exit 1
fi

cd "$TERRAFORM_DIR"

# Initialize Terraform
log_info "Initializing Terraform..."
terraform init -upgrade

# Build Python package (emr_helpers.zip) with boto3
log_info "Building Python package with boto3..."
cd "$PROJECT_ROOT/app"
mkdir -p dependencies

# Create a temporary directory to install boto3
TEMP_DIR=$(mktemp -d)
pip3 install boto3 -t "$TEMP_DIR" --quiet

# Build zip with boto3 included using a Python script file
cat > build_zip.py << 'PYEOF'
import zipfile
import os

temp_dir = os.environ['TEMP_DIR']

with zipfile.ZipFile('dependencies/emr_helpers.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
    # Add project source files
    for root, dirs, files in os.walk('src/dependencies'):
        for file in files:
            if not file.endswith('.pyc') and '__pycache__' not in root:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, 'src')
                zipf.write(filepath, arcname)
    for root, dirs, files in os.walk('src/jobs'):
        for file in files:
            if not file.endswith('.pyc') and '__pycache__' not in root:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, 'src')
                zipf.write(filepath, arcname)
    # Add boto3 and its dependencies
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            filepath = os.path.join(root, file)
            arcname = os.path.relpath(filepath, temp_dir)
            zipf.write(filepath, arcname)
PYEOF

TEMP_DIR="$TEMP_DIR" python3 build_zip.py
rm build_zip.py

# Cleanup
rm -rf "$TEMP_DIR"

log_info "Python package built: dependencies/emr_helpers.zip"

# Upload artifacts to S3 (Terraform will also do this via aws_s3_object)
# But we do it here to ensure latest code is available
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
WORKSPACE_BUCKET="lakehouse-workspace-${ACCOUNT_ID}"

log_info "Uploading artifacts to s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/..."

aws s3 cp "$PROJECT_ROOT/app/src/main.py" \
    "s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/jobs/main.py"

aws s3 cp "$PROJECT_ROOT/app/src/dependencies/config/tables_config.json" \
    "s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/config/tables_config.json"

aws s3 sync "$PROJECT_ROOT/app/src/dependencies/sql/" \
    "s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/dependencies/sql/"

aws s3 sync "$PROJECT_ROOT/app/src/dependencies/" \
    "s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/dependencies/" \
    --include "*.py" --exclude "*/__pycache__/*"

aws s3 sync "$PROJECT_ROOT/app/src/jobs/" \
    "s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/dependencies/jobs/" \
    --include "*.py" --exclude "*/__pycache__/*"

aws s3 cp "$PROJECT_ROOT/app/dependencies/emr_helpers.zip" \
    "s3://${WORKSPACE_BUCKET}/aws-emr/flight-radar/dependencies/emr_helpers.zip"

log_info "Artifacts uploaded successfully"

# Run Terraform
cd "$TERRAFORM_DIR"

case "$ACTION" in
    plan)
        log_info "Running terraform plan..."
        terraform plan -var-file="$TFVARS_FILE"
        ;;
    apply)
        log_info "Running terraform apply..."
        terraform apply -var-file="$TFVARS_FILE" -auto-approve
        log_info "Deploy completed successfully!"
        log_info "State machine ARN: $(terraform output -raw state_machine_arn)"
        ;;
esac