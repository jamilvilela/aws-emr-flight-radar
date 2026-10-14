# SDD Execution Guide — EMR Silver Layer

Order to implement the specification documents in `docs-sdd-emr/`.

---

## Phase 1 — Foundation (Config & Schema)

| Step | File | Action |
|------|------|--------|
| 1.1 | `tables_config_schema.md` | Review schema; create `tables_config.json` with all 10 tables |
| 1.2 | `silver_modeling.md` | Validate column mappings, PK/SK, partitioning against `tables_config.json` |
| 1.3 | `spec-emr.md` §2.1–2.3 | Confirm project structure, CLI args, table inventory |

**Deliverable:** `app/aws-emr/src/dependencies/config/tables_config.json` + `sql/*.sql` stubs

---

## Phase 2 — Core Python Modules

| Step | File | Module |
|------|------|--------|
| 2.1 | `spec-emr.md` §3.1 | `config_models.py` — `TablesConfig`, `TableConfig`, `from_s3()` |
| 2.2 | `spec-emr.md` §3.2 | `reader.py` — `read_table()` with partition pruning |
| 2.3 | `spec-emr.md` §3.3 | `transformer.py` — dispatcher (Spark SQL / PySpark) |
| 2.4 | `spec-emr.md` §3.4 | `writer.py` — `merge_delta()`, bootstrap, `write_rejects()` |
| 2.5 | `spec-emr.md` §3.5 | `data_quality.py` — PK/enum/FK/CDC checks |
| 2.6 | `spec-emr.md` §3.6–3.8 | `rejected_records.py`, `etl_control.py`, `quality_metrics.py` (target `db_raw.*`) |
| 2.7 | `spec-emr.md` §3.9 | `aws_helper.py` — boto3 utilities |
| 2.8 | `spec-emr.md` §3.10 | `main.py` — argparse, Spark init, table resolution, pipeline loop |

**Deliverable:** Working `main.py` + all dependencies under `src/dependencies/`

---

## Phase 3 — SQL & PySpark Transformations

| Step | Tables | Type |
|------|--------|------|
| 3.1 | `dim_aircraft_type`, `dim_aircraft`, `dim_airport`, `dim_airline`, `dim_country`, `dim_route`, `dim_date` | `spark_sql` → `sql/*.sql` |
| 3.2 | `fact_flight`, `fact_flight_position` | `pyspark` → `jobs/process_fact_*.py` |

Use `silver_modeling.md` §2–3 for exact column mappings and join logic.

---

## Phase 4 — Infrastructure (Terraform)

| Step | File | Resources |
|------|------|-----------|
| 4.1 | `emr_terraform_spec.md` §1–3 | `locals.tf`, `variables.tf`, `versions.tf` |
| 4.2 | `emr_terraform_spec.md` §4 | `emr_cluster.tf` — cluster, managed scaling, auto-termination |
| 4.3 | `emr_terraform_spec.md` §5 | `emr_step.tf` — `spark-submit` with `local.spark_args` |
| 4.4 | `emr_terraform_spec.md` §6–7 | `emr_security.tf`, `iam.tf` — KMS, security config, roles, Lake Formation grants |
| 4.5 | `emr_terraform_spec.md` §8–10 | `s3.tf`, `cloudwatch.tf`, `stepfunctions.tf`, `outputs.tf` |

**Deliverable:** `infra/emr/` module, `terraform.tfvars.example`

---

## Phase 5 — Tests

| Step | File | Scope |
|------|------|-------|
| 5.1 | `plan-emr.md` §5 | Unit tests: config, reader, transformer, DQ, writer, main args |
| 5.2 | `plan-emr.md` §5 | Integration: EMR step, E2E pipeline (small cluster / EMR Serverless) |

---

## Phase 6 — CI/CD & Deploy

| Step | File | Script |
|------|------|--------|
| 6.1 | `plan-emr.md` §5 | `ci-cd/deploy-emr.sh` — build zip, terraform apply, upload artifacts |
| 6.2 | `plan-emr.md` §5 | `ci-cd/rollback-emr.sh` — terraform destroy (preserve data) |
| 6.3 | `deployment_guide.md` §2–3 | Validate manual run, Step Functions, backfill |

---

## Phase 7 — Go-Live Checklist

Per `deployment_guide.md` §8:

- [ ] `db_trusted` exists + Lake Formation grants
- [ ] Buckets created (raw, trusted, workspace, logs)
- [ ] Artifacts published to workspace bucket
- [ ] Single-table validation on small/spot cluster
- [ ] EventBridge + Step Functions active
- [ ] CloudWatch alarms → Slack/PagerDuty

---

## Reference Map

| Document | Purpose |
|----------|---------|
| `spec-emr.md` | Single source of truth: architecture, classes, CLI, writer, infra summary |
| `tables_config_schema.md` | Contract schema for `tables_config.json` |
| `silver_modeling.md` | Bronze→Silver column mappings, DDL, ER diagram |
| `feature-emr.md` | Feature overview, pipeline mermaid, quality rules, support table schemas |
| `plan-emr.md` | Project structure, implementation phases, deliverables tracker |
| `emr_terraform_spec.md` | Terraform module spec (all `.tf` files) |
| `deployment_guide.md` | Build, deploy, run, troubleshoot, operations |
| `arquitetura-emr.md` | Architecture decisions, medallion, security, SLA, risks |

---

## Quick Start Commands

```bash
# 1. Create tables_config.json from schema
cd /c/repos/aws-emr-flight-radar
# edit docs-sdd-emr/tables_config_schema.md examples → app/aws-emr/src/dependencies/config/tables_config.json

# 2. Scaffold Python structure
mkdir -p app/aws-emr/src/{dependencies/{config,sql},jobs,tests/{unit,integration}}

# 3. Implement Phase 2 modules in order

# 4. Deploy infra
cd infra/emr
terraform init && terraform apply -var-file=terraform.tfvars

# 5. Run via Step Functions
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"processing_date":"2026-09-19","tables":"all","mode":"incremental"}'
```
