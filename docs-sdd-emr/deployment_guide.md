---
id: deployment-guide-emr
title: Guia de Deploy & Operação — EMR Trusted
status: draft
version: 1.0
created: 2026-09-14
updated: 2026-09-14
author: Data Engineering Team
---

# Guia de Deploy & Operação — EMR Trusted (db_trusted)

## 1. Pré-requisitos

| Item | Valor |
|---|---|
| Região | `us-east-1` (default) |
| VPC | default + subnets privadas (reuso do `infra/data.tf`) |
| Catalog | `db_raw` (leitura) e `db_trusted` (escrita) existentes |
| Lake Formation | grants `SELECT` (db_raw) / `ALL` (db_trusted) para a role EMR |
| Buckets | `lakehouse-raw-{acct}`, `lakehouse-trusted-{acct}`, `lakehouse-workspace-{acct}`, `lakehouse-logs-{acct}` |
| CLI | AWS CLI, Terraform ≥ 1.5, jq |

## 2. Build & Deploy

### 2.1 Publicar artefatos no workspace

O Terraform sobe os arquivos via `aws_s3_object` (idempotente, sem upload manual):

```bash
# build do package Python (emr_helpers.zip)
python -m zipfile -c app/dependencies/emr_helpers.zip \
  -r app/src/dependencies/ \
  -r app/src/jobs/
```

### 2.2 Aplicar Terraform

```bash
cd infra/emr
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars -auto-approve
```

`terraform.tfvars` (criado a partir do example):

```hcl
region            = "us-east-1"
environment       = "dev"
processing_tables = "all"
processing_mode   = "incremental"
processing_date   = ""

emr_cluster_config = {
  name                  = "flight-radar-etl"
  release_label         = "emr-7.5.0"
  applications          = ["Spark", "Hive"]
  instance_type_master  = "m6g.xlarge"
  instance_type_core    = "m6g.2xlarge"
  instance_type_task    = "m6g.2xlarge"
  core_instance_count   = 3
  task_instance_count   = 0
  core_ebs_size_gb      = 100
  task_ebs_size_gb      = 200
  auto_termination_idle = 30
  managed_scaling       = true
  min_capacity          = 2
  max_capacity          = 10
}

raw_database     = "db_raw"
trusted_database = "db_trusted"
raw_bucket       = ""
trusted_bucket   = ""
checkpoint_bucket = ""
log_level        = "INFO"

tags = {
  Project     = "flight-radar"
  Environment = "dev"
  Owner       = "data-engineering"
}
```

> Aplique com `environment` (`dev`, `prod`) via `ci-cd/deploy-emr.sh`.

## 3. Execução

### 3.1 Via Step Functions (padrão)

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:run-etl-dev \
  --input '{"processing_date":"2026-09-14","tables":"all","mode":"incremental"}'
```

A EventBridge dispara a cada hora com a `processing_date` atual.

### 3.2 Via EMR Step manual (debug)

```bash
aws emr add-step \
  --cluster-id j-XXXXXXXX \
  --steps "Type=CUSTOM_JAR,Name=ETL,ActionOnFailure=TERMINATE_CLUSTER,Jar=command-runner.jar,Args=[...]" 
```

### 3.3 Backfill (primeira carga / reprocessamento)

```bash
# full: reprocessa todo o histórico
processing_mode="full" processing_tables="all" terraform apply -var-file=terraform.tfvars

# ou via Step Functions
aws stepfunctions start-execution \
  --state-machine-arn ... \
  --input '{"processing_date":"2026-01-01","tables":"all","mode":"full"}'
```

### 3.4 Processar tabela específica

```bash
processing_tables="flights" processing_date="2026-09-14" processing_mode="incremental"
```

## 4. Parametrização (resumo)

| Parâmetro | Default | Observação |
|---|---|---|
| `--tables` | `all` | uma, várias (CSV) ou `all` |
| `--mode` | `incremental` | `full` para backfill |
| `--processing_date` | hoje UTC | obrigatório em `incremental` |
| `--raw_database` / `--trusted_database` | `db_raw` / `db_trusted` | — |
| `--log_level` | `INFO` | `DEBUG` para diagnóstico |

## 5. Logs e Observabilidade

| Fonte | Local |
|---|---|
| Logs do cluster EMR | `s3://lakehouse-logs-{acct}/emr/cluster-logs/` |
| Spark Event Logs (Spark UI) | `s3://lakehouse-logs-{acct}/emr/spark-events/` |
| CloudWatch Logs | `/aws/emr/flight-radar-etl` |
| Métricas custom | namespace `FlightRadar` (`ETL.RowsWritten`, `ETL.Duration`, `ETL.Failed`) |
| Alarmes | falha de step, duração > SLA, qualidade < threshold |

### Acessar Spark UI (histórico)

Com o Spark History Server (ou EMR standalone UI), aponte `spark.history.fs.logDirectory`
para `s3://lakehouse-logs-{acct}/emr/spark-events/`.

## 6. Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| `ClassNotFoundException: ...delta...` | Artefato Delta ausente no classpath | Adicionar `io.delta:delta-spark_2.13:4.0.0` no `--packages` ou bootstrap |
| `S3SingleDriverLogStore` erro | `spark.delta.logStore.class` não setado | checar `--conf` no step |
| `db_trusted` does not exist | catalog/database não criado/permissionado | criar DB + grants Lake Formation |
| step falha rápido | PK null / enum inválido | ver `db_raw.rejected_records` |
| posições lentas | leitura sem range `recorded_at` | incremental de posições usa range watermark |
| Spark UI vazio | `spark.eventLog.dir` não configurado | revisar `local.spark_args` |
| `RuntimeError: Table not found` | Tabela não existe no Glue Catalog | Tabelas devem ser pré-criadas |

## 7. Operações

### 7.1 Idempotência

O `MERGE` por natural key permite re-executar o mesmo `processing_date` sem duplicar. Para
reprocessar um dia inteiro, use `--mode full --processing_date <dia>` na tabela desejada.

### 7.2 VACUUM e retenção

`VACUUM db_trusted.<table> RETAIN 168 HOURS` (7 dias) — configurável, executado no fim do job
para fatos.

### 7.3 Rollback de ambiente

`ci-cd/rollback-emr.sh` → `terraform destroy` (não remove dados Trusted do S3).

## 8. Checklist de go-live

- [ ] `db_trusted` criado + grants Lake Formation para a role EMR.
- [ ] buckets criados (raw, trusted, workspace, logs).
- [ ] artefatos publicados no workspace bucket.
- [ ] validação com cluster pequeno (spot) em tabela única.
- [ ] EventBridge + Step Functions ativas.
- [ ] alarmes CloudWatch acionam Slack/PD.

---

**Fim do documento.** Salve como `deployment_guide.md`.