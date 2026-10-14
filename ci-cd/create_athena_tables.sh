#!/usr/bin/env bash
# Athena Table Creation Script
# Usage: ./create_athena_tables.sh <account_id> [region]
# Example: ./create_athena_tables.sh 123456789012 us-east-1

set -euo pipefail

ACCOUNT_ID="${1:-}"
REGION="${2:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/../docs-sdd-emr/athena_create_tables.sql"

if [[ -z "$ACCOUNT_ID" ]]; then
    echo "Usage: $0 <account_id> [region]"
    echo "Example: $0 123456789012 us-east-1"
    exit 1
fi

if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: SQL file not found at $SQL_FILE"
    exit 1
fi

echo "Creating Athena tables for account: $ACCOUNT_ID in region: $REGION"

# Replace account_id placeholder
sed "s/\${account_id}/$ACCOUNT_ID/g" "$SQL_FILE" > "/tmp/athena_tables_${ACCOUNT_ID}.sql"

echo "Generated SQL file: /tmp/athena_tables_${ACCOUNT_ID}.sql"
echo ""
echo "To execute in Athena:"
echo "1. Open Athena console in region $REGION"
echo "2. Copy/paste the contents of /tmp/athena_tables_${ACCOUNT_ID}.sql"
echo "3. Or use AWS CLI:"
echo "   aws athena start-query-execution \\"
echo "     --query-string \"\$(cat /tmp/athena_tables_${ACCOUNT_ID}.sql)\" \\"
echo "     --work-group primary \\"
echo "     --result-configuration OutputLocation=s3://lakehouse-logs-${ACCOUNT_ID}/athena-results/"
echo ""
echo "Note: Tables must already exist in Glue Catalog (created by infrastructure process)"
echo "This script only registers them in Athena for querying."