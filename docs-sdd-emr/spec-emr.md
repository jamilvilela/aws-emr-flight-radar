---
id: spec-emr-silver
title: Specification — EMR Spark 4.0 Trusted Layer (db_trusted)
status: draft
version: 1.0
created: 2026-09-14
updated: 2026-09-14
author: Data Engineering Team
---

# Specification — EMR Trusted Layer (Bronze → Trusted)

## 1. Overview

Pipeline de processamento **batch on-demand** executado em **Amazon EMR 7.5.0** com
**Spark 4.0** + **Delta Lake 4.0**. Lê tabelas consolidadas do **Bronze** (`db_raw.fr_*`, Delta),
aplica transformações de negócio e escreve **tabelas modeladas** na camada **Trusted**
(`db_trusted`) em Delta Lake.

A aplicação é **parametrizada** para processar uma ou mais tabelas (ou `all`), em dois modos:

- **`full`**: reprocessa todo o histórico (backfill). Ignora filtro de `processing_date`
  (ou usa a data base do backfill).
- **`incremental`**: processa apenas a partição (`event_date` / range `recorded_at`) da
  `--processing_date` (padrão: data atual). É o modo da execução horária.

### Decisões Alinhadas

| Pergunta | Decisão |
|---|---|
| Orquestração | **AWS Step Functions** (State Machine `run-etl`) |
| Frequência | **Horária** (incremental) + backfill manual (`full`) |
| Volumetria | full = 5 GB parquet/delta; CDC = ~150 MB/min |
| SLA | incremental ≤ 20 min; full ≤ 20 min |
| Custo | sem restrição (Graviton m6g/r6g; spot opcional) |
| EMR release | **emr-7.5.0** (Spark 4.0) — validar aplicativos na região |
| Delta Lake | **Delta 4.0** (`io.delta:delta-spark_2.13:4.0.0`) |
| Catálogo | **Glue Data Catalog** (`db_raw`, `db_trusted`) |
| Governança | Lake Formation já configurado; criar roles/policies deste projeto |
| Backfill | Sim (`--mode full --processing_date <base>`) |
| CDC na Trusted | Sim — full (backfill) e incremental (MERGE idempotente) |
| Homologação | Sem ambiente separado — validar cluster pequeno/spot |

### Technology Stack

| Componente | Versão |
|---|---|
| Amazon EMR | emr-7.5.0 |
| Apache Spark | 4.0 (Scala 2.13) |
| Delta Lake | 4.0 (`io.delta:delta-spark_2.13:4.0.0`) |
| Python | 3.9 |
| Formato destino | Delta Lake (Snappy) |
| Catálogo | Glue Data Catalog + Lake Formation |
| Orquestração | AWS Step Functions + EventBridge |
| IaC | Terraform ≥ 1.5 |
| Testes | pytest + pyspark (unit) / boto3 (integration) |

## 2. Arquitetura do Job

### 2.1 Estrutura do Código (`app/`)

```
app/
├── src/
│   ├── main.py                      # Entry point (argparse + Spark init + PipelineRunner)
│   ├── __init__.py
│   ├── dependencies/
│   │   ├── __init__.py
│   │   ├── config_models.py         # Dataclasses (TablesConfig, TableConfig)
│   │   ├── config/
│   │   │   └── tables_config.json   # Tables Config (contrato por tabela)
│   │   ├── sql/
│   │   │   ├── dim_aircraft.sql
│   │   │   ├── dim_airport.sql
│   │   │   ├── dim_airline.sql
│   │   │   ├── dim_route.sql
│   │   │   └── ...                  # transformações Spark SQL
│   │   ├── reader.py                # Leitura db_raw (Delta via Glue Catalog)
│   │   ├── transformer.py           # Transformações Spark SQL + PySpark
│   │   ├── writer.py                # Escrita db_trusted (Delta MERGE only)
│   │   ├── data_quality.py          # Validações Trusted (null PK, enum, FK)
│   │   ├── etl_control.py           # Log de execução (register)
│   │   ├── quality_metrics.py       # Métricas de qualidade (save)
│   │   ├── rejected_records.py      # Registros rejeitados centralizados
│   │   ├── aws_helper.py            # Utilitários boto3 (S3, CloudWatch, Glue)
│   │   └── pipeline.py              # Pipeline e PipelineRunner classes
│   └── jobs/
│       ├── __init__.py
│       ├── process_dim_aircraft.py
│       ├── process_dim_airport.py
│       ├── process_dim_airline.py
│       ├── process_dim_route.py
│       ├── process_dim_country.py
│       ├── process_dim_date.py
│       ├── process_fact_flight.py
│       └── process_fact_flight_position.py
└── tests/
    ├── unit/
    └── integration/
```

> `sql/` opcional: se `transform_type == "spark_sql"` no `tables_config.json`, a
> transformação vem de um arquivo `.sql` no workspace bucket. Se `pyspark`, vem de
> um módulo em `jobs/`.

### 2.2 Parâmetros de Entrada (argparse)

```python
parser = argparse.ArgumentParser(description="EMR ETL (Bronze -> Trusted)")
parser.add_argument("--tables", required=True,
    help="Lista de tabelas a processar (ex: airports,flights ou all)")
parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
parser.add_argument("--processing_date", help="Data de processamento (YYYY-MM-DD)")
parser.add_argument("--config_s3_path", required=True, help="S3 path do tables_config.json")
parser.add_argument("--raw_database", default="db_raw", help="Database origem (Bronze)")
parser.add_argument("--trusted_database", default="db_trusted", help="Database destino (Trusted)")
parser.add_argument("--log_level", choices=["DEBUG", "INFO", "WARN"], default="INFO")
```

**Regras de `--tables`:**
- `all` → processa todas as tabelas do `tables_config.json` na ordem de `order`.
- `airports,flights` → processa apenas as listadas (e suas dependências).
- `--mode full`: ignora `processing_date` (processa todo o histórico das fontes).
- `--mode incremental`: exige `--processing_date` (default = hoje UTC).

### 2.3 Modelo de Tabelas Trusted

| Tabela Trusted | Fonte(s) db_raw | PK (merge) | SK (surrogate) | Partição |
|---|---|---|---|---|
| `dim_aircraft_type` | `fr_aircraft_types` | `icao_code` | `aircraft_type_key` | `event_date` |
| `dim_aircraft` | `fr_aircraft` ⋈ `fr_aircraft_types` | `icao24` | `aircraft_key` | `event_date` |
| `dim_airport` | `fr_airports` | `icao_code` | `airport_key` | `event_date` |
| `dim_airline` | `fr_airlines` | `icao_code` | `airline_key` | `event_date` |
| `dim_country` | `fr_countries` | `id` | `country_key` | `event_date` |
| `dim_route` | `fr_routes` ⋈ `fr_airports` | `id` | `route_key` | `event_date` |
| `dim_date` | gerada | `date_key` | — | — |
| `fact_flight` | `fr_flights` ⋈ dims | `flight_id` | — | `event_date` (`scheduled_departure`) |
| `fact_flight_position` | `fr_aircraft_positions` ⋈ dims | `position_id`,`recorded_at` | — | `event_date` (`recorded_at`) |

### 2.4 Lógica de Transformação

**Surrogate key (hash determinístico):**

```python
from pyspark.sql import functions as F

def surrogate_key(col_name: str) -> str:
    return F.xxhash64(F.col(col_name)).cast("bigint")
```

**dim_aircraft (Spark SQL) — SCD Tipo 1 (estado vigente):**

```sql
SELECT
  xxhash64(a.icao24)                            AS aircraft_key,
  a.icao24,
  a.registration,
  a.aircraft_type,
  at.iata_code                                  AS type_iata_code,
  at.name AS type_name,
  at.manufacturer,
  a.serial_number,
  a.operator_icao,
  a.operator_name,
  a.year_built,
  a.created_at,
  a.updated_at,
  current_timestamp()                           AS trusted_ingestion_ts,
  'EMR_SPARK'                                   AS trusted_process_engine,
  date(a.updated_at)                            AS event_date
FROM db_raw.fr_aircraft a
LEFT JOIN db_raw.fr_aircraft_types at ON a.aircraft_type = at.icao_code
WHERE a.cdc_operation <> 'D'
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.icao24 ORDER BY a.updated_at DESC NULLS LAST) = 1;
```

> `QUALIFY` remove duplicatas por natural key deixando o estado mais recente
> (SCD Tipo 1). O `MERGE` (Writer) garante a idempotência.

**fact_flight (PySpark API — join com dimensões para chaves substitutas):**

```python
def transform_fact_flight(spark, raw_db, trusted_db, processing_date, mode):
    flights = spark.table(f"{raw_db}.fr_flights").filter(F.col("cdc_operation") != "D")
    if mode == "incremental":
        flights = flights.filter(F.col("event_date") == processing_date)

    dim_airline = spark.table(f"{trusted_db}.dim_airline")
    dim_aircraft = spark.table(f"{trusted_db}.dim_aircraft")
    dim_airport = spark.table(f"{trusted_db}.dim_airport")
    dim_route = spark.table(f"{trusted_db}.dim_route")

    orig = dim_airport.alias("orig")
    dest = dim_airport.alias("dest")

    fact = (flights
        .join(dim_airline, flights.airline_icao == dim_airline.icao_code, "left")
        .join(dim_aircraft, flights.aircraft_icao24 == dim_aircraft.icao24, "left")
        .join(orig, flights.origin_airport == orig.icao_code, "left")
        .join(dest, flights.destination_airport == dest.icao_code, "left")
        .join(
            dim_route,
            (orig.icao_code == dim_route.origin_icao)
                & (dest.icao_code == dim_route.destination_icao),
            "left",
        )
        .select(
            F.col("flight_id"),
            F.col("flight_number"),
            dim_airline.airline_key,
            dim_aircraft.aircraft_key,
            orig.airport_key.alias("origin_airport_key"),
            dest.airport_key.alias("destination_airport_key"),
            dim_route.route_key,
            F.col("scheduled_departure"),
            F.col("scheduled_arrival"),
            F.col("actual_departure"),
            F.col("actual_arrival"),
            F.col("status"),
            F.current_timestamp().alias("source_ingestion_ts"),
            F.lit("EMR_SPARK").alias("trusted_process_engine"),
            F.to_date(F.col("scheduled_departure")).alias("event_date"),
        )
    )
    return fact
```

### 2.5 Writer (Delta MERGE por Natural Key)

Toda escrita Trusted usa `DeltaTable.forName()` via Glue Data Catalog + `MERGE`.
**Tabelas são pré-criadas no Glue Catalog pelo processo de infraestrutura.** O Writer
apenas manipula dados, não metadados.

```python
from delta.tables import DeltaTable

def merge_delta(delta, updates, keys):
    cond = " AND ".join([f"t.{k} = u.{k}" for k in keys])
    delta.alias("t").merge(updates.alias("u"), cond) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
```

**Estrutura em `writer.py`:**
1. `DeltaTable.forName(spark, "db_trusted.table")` — leitura de metadados (read-only).
2. Se tabela não existir: **falha com erro claro** — tabelas devem ser pré-criadas.
3. `merge_delta(...)`: `WHEN NOT MATCHED THEN INSERT` / `WHEN MATCHED THEN UPDATE`.
4. `write_rejects(rejects_df)` → `db_raw.rejected_records` (MERGE append).
5. `OPTIMIZE ... ZORDER BY (...)` pós-escrita (fatos).
6. `VACUUM` retenção de 7 dias (configurável).

### 2.6 Escrita Delta (Parâmetros)

- **Formato:** Delta Lake (`spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog`).
- **Particionamento:** `event_date` (dims e fatos) — ver `silver_modeling.md`.
- **MERGE por natural key**: `dim_*` por NK; `fact_flight` por `flight_id`; `fact_flight_position` por `(position_id, recorded_at)`.
- **Dedup:** `MERGE` + `QUALIFY ROW_NUMBER()` na view (estado vigente).
- **Otimização:** `OPTIMIZE dim_*` (coalesce), fatos `ZORDER BY`; `AUTO OPTIMIZE` (delta).

## 3. Componentes (classes Python)

### 3.1 `config_models.py`

Dataclasses:

| Classe | Campos |
|---|---|
| `ColumnSpec` | name, type, nullable, comment |
| `PartitionSpec` | name, type, source_column |
| `SurrogateKeySpec` | name, col |
| `TableConfig` | target_table, source_tables, primary_key, surrogate_key, merge_fallback_keys, partition_keys, transform_type, transform_query, transform_module, dependencies, order, enable_zorder, zorder_cols, enum_columns |
| `TablesConfig` | `_tables: List[TableConfig]`, `tables` (by order), `get_table(name)`, `from_s3()`, `resolve_tables()`, `to_dict()` |

`from_s3()` lê `s3://.../tables_config.json` e valida dependências (ex.: `fact_flight` depende de `dim_airline`, etc.).
`resolve_tables()`: `all` → todas por `order`; lista → topological sort por `dependencies`.

### 3.2 `reader.py` — Reader

- `read_table(spark, database, table, processing_date, mode)` → `spark.table(f"{database}.{table}")`
- Aplica partition prune: dimensões/fatos por `event_date`; posições por `recorded_at`.
- Renomeia colunas CDC: `Op`→`cdc_operation`; `dms_timestamp`→`cdc_timestamp`.

### 3.3 `transformer.py` — Transformer

- `transform(spark, table: TableConfig, raw_db, trusted_db, ...)` → DataFrame
- Dispatcher: `transform_type == "spark_sql"` (executa SQL do arquivo) vs `pyspark` (importa módulo `jobs.`).
- Retorna DF pronto para `writer`.

### 3.4 `writer.py` — Writer

- `DeltaTable.forName(spark, full_name)` — resolve tabela existente (read-only).
- **Não cria tabelas** — falha se tabela não existir no Glue Catalog.
- `merge_delta()`: MERGE com `whenMatchedUpdateAll()` + `whenNotMatchedInsertAll()`.
- `write_rejects()` → `db_raw.rejected_records` via MERGE append.

### 3.5 `data_quality.py` — DataQuality (Trusted)

Validações Trusted:

| Estágio | Method | Descrição |
|---|---|---|
| 1 | `check_pk_not_null` | Remove registros com natural key null |
| 2 | `check_cdc_operation` | Descarta `Op = 'D'` (soft delete) |
| 3 | `check_enums` | Valida colunas enumeradas (`status` em flights) |
| 4 | `check_fks` | Valida se FKs resolvem para dimensões (left join null check) |

Retorna `(valid_df, rejects_df)` com `_reject_rule`, `_reject_reason`.

### 3.6 `rejected_records.py` — Centralização

- Tabela `db_raw.rejected_records` (Delta) — `rejected_record_json` + metadata
  (source/target database/table, reject rule/reason, execution_id, reference_date).
- Append-only `partitionBy("reference_date")`.

### 3.7 `etl_control.py` — `register()` → `db_raw.etl_control`

Schema: `execution_id, job_name, source, target_table, mode, execution_start, execution_end,
status, records_read, records_written, records_rejected, error_message, reference_date`.

### 3.8 `quality_metrics.py` — `save()` → `db_raw.quality_metrics`

Schema: `database, table, processing_timestamp, metric, rule, status, failure_reason,
partition, technology, reference_date`.

### 3.9 `aws_helper.py` — utilitários boto3

`get_account_id()`, `get_json_from_s3()`, `put_metric()`, `put_log()`,
`get_table_location()`, `ensure_glue_database()`, `build_s3_path()`.

### 3.10 `pipeline.py` — Pipeline Orchestration

| Classe | Responsabilidade |
|---|---|
| `Pipeline` | Executa ETL completo para uma tabela (Reader → Transformer → DQ → Writer → EtlControl/QualityMetrics) |
| `PipelineRunner` | Orquestra múltiplas tabelas (resolve dependências, executa em ordem) |

### 3.11 `main.py` — Entry Point (thin)

```
main():
  1. _parse_args() (argparse)
  2. _init_spark() (Delta extensions via `--conf`)
  3. TablesConfig.from_s3(config_s3_path)
  4. _resolve_processing_date(args)
  5. PipelineRunner(spark, config, raw_db, trusted_db, mode, processing_date).run(args.tables)
```

## 4. Infraestrutura (Terraform) — resumo

| Grupo | Recurso | Descrição |
|---|---|---|
| Cluster | `aws_emr_cluster.main` | emr-7.5.0, Spark + Delta, subnets privadas, managed scaling |
| Security | `aws_emr_security_configuration` | encryption (S3, EBS, logs) |
| IAM | `aws_emr_service_role` / EC2 role / instance profile / policies | least privilege |
| S3 | artifacts / logs | Spark Event Logs + cluster logs |
| VPC | subnets privadas, SG, endpoints | reutiliza VPC default |
| Metrics | `aws_cloudwatch_log_group` + alarms | observabilidade |
| Orquestração | Step Functions + EventBridge (rule) | `run-etl` |
| Variáveis | `emr_cluster_config` object | parametrização do cluster |

Spark `--conf` importantes no step:

```
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.shuffle.partitions=200
spark.executor.memory=8g
spark.executor.cores=4
spark.driver.memory=4g
spark.databricks.delta.optimizeWrite.enabled=true
spark.databricks.delta.autoCompact.enabled=true
spark.eventLog.enabled=true
spark.eventLog.dir=s3://${logs_bucket}/emr/spark-events/
```

## 5. Testes

- **Unit** (`tests/unit/`): `test_config.py`, `test_reader.py`, `test_transformer.py`,
  `test_data_quality.py`, `test_writer.py`, `test_main_args.py` (argparse).
- **Integration** (`tests/integration/`): `conftest.py` (boto3, Glue Catalog/S3), `test_emr_step.py`,
  `test_pipeline_e2e.py` (cluster pequeno/EMR Serverless).

## 6. Parâmetros do Step (resumo)

| parâmetro | required |
|---|---|
| `--tables` | sim |
| `--mode` | não (default incremental) |
| `--processing_date` | mode=incremental |
| `--config_s3_path` | sim |
| `--raw_database` / `--trusted_database` | não (db_raw / db_trusted) |
| `--log_level` | não (INFO) |

---

**Fim do documento.** Salve como `spec-emr.md`.