---
id: tables-config-schema
title: Schema — tables_config.json
status: draft
version: 1.0
created: 2026-09-14
updated: 2026-09-14
author: Data Engineering Team
---

# Schema do `tables_config.json`

Arquivo único (lista de tabelas) persistido em
`s3://lakehouse-workspace-{account_id}/aws-emr/flight-radar/config/tables_config.json`.
Define o **contrato de dados** da camada Trusted: origem, chaves, particionamento,
transformação e dependências.

## 1. Estrutura geral

```json
[
  {
    "target_table": "string",
    "source_tables": ["string"],
    "primary_key": ["string"],
    "surrogate_key": { "name": "string", "col": "string" },
    "merge_fallback_keys": ["string"],
    "partition_keys": [ { "name": "string", "type": "date|string|int", "source_column": "string" } ],
    "transform_type": "spark_sql | pyspark",
    "transform_query": "string",       // quando spark_sql
    "transform_module": "string",      // quando pyspark
    "dependencies": ["string"],
    "order": 1,
    "enable_zorder": true,
    "zorder_cols": ["string"],
    "enum_columns": { "col": ["a", "b"] }
  }
]
```

## 2. Campos

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `target_table` | string | sim | Nome da tabela em `db_trusted` (ex.: `dim_aircraft`) |
| `source_tables` | string[] | sim | Tabelas Bronze lidas em `db_raw` |
| `primary_key` | string[] | sim | Natural key usada no `MERGE` |
| `surrogate_key` | object | não | `{ name, col }` — coluna SK derivada (`xxhash64`) |
| `merge_fallback_keys` | string[] | não | chaves alternativas de merge (ex.: aeroporto sem ICAO) |
| `partition_keys` | object[] | sim | partições (nome + tipo + coluna fonte) |
| `event_type` | string | não | tipo do campo `event_date` (padrão: `coalesce(updated_at,created_at,processing_date)`) |
| `transform_type` | `spark_sql`\|`pyspark` | sim | tipo da transformação |
| `transform_query` | string | condicional | path relativo do `.sql` no workspace (quando spark_sql) |
| `transform_module` | string | condicional | módulo Python (quando pyspark) |
| `dependencies` | string[] | não | tabelas Trusted que devem rodar antes |
| `order` | int | sim | ordem global de execução |
| `enable_zorder` | bool | não | aplicar `ZORDER` pós-escrita (fatos) |
| `zorder_cols` | string[] | condicional | colunas do `ZORDER` |
| `enum_columns` | object | não | coluna → valores permitidos |

## 3. Tipos permitidos

- `transform_type`: literal `spark_sql` | `pyspark`.
- `partition_keys[].type`: `date` | `string` | `int`.
- `surrogate_key.col`: string (uma coluna) para `xxhash64`.

## 4. Exemplo completo (2 registros)

```json
[
  {
    "target_table": "dim_aircraft",
    "source_tables": ["fr_aircraft", "fr_aircraft_types"],
    "primary_key": ["icao24"],
    "surrogate_key": { "name": "aircraft_key", "col": "icao24" },
    "partition_keys": [{ "name": "event_date", "type": "date" }],
    "transform_type": "spark_sql",
    "transform_query": "sql/dim_aircraft.sql",
    "dependencies": ["dim_aircraft_type"],
    "order": 2
  },
  {
    "target_table": "fact_flight",
    "source_tables": ["fr_flights"],
    "primary_key": ["flight_id"],
    "partition_keys": [{ "name": "event_date", "type": "date", "source_column": "scheduled_departure" }],
    "transform_type": "pyspark",
    "transform_module": "jobs.process_fact_flight",
    "dependencies": ["dim_airline", "dim_aircraft", "dim_airport", "dim_route"],
    "order": 10,
    "enable_zorder": true,
    "zorder_cols": ["flight_id"],
    "enum_columns": { "status": ["scheduled", "active", "landed", "cancelled", "diverted"] }
  }
]
```

## 5. Regras e validações (em `TablesConfig.from_s3`)

1. `target_table` e `primary_key` obrigatórios e únicos por arquivo.
2. Se `transform_type == "spark_sql"` → `transform_query` deve existir; caso `pyspark` → `transform_module`.
3. `dependencies` deve referenciar somente tabelas já presentes no arquivo (else vira erro de configuração).
4. `order` não pode ser duplicado; `TablesConfig.tables` retorna a lista ordenada por `order`.
5. Se `enable_zorder` é `true` → `zorder_cols` obrigatório.
6. `source_tables` usado para log/auditoria e para `reader` (não para o join em si — a transform carrega via SQL/PySpark).

## 6. Relação com `--mode` e `--processing_date`

- `--mode incremental`: `TablesConfig` injeta filtro de partition (`event_date == processing_date`
  para dims/fatos; range `recorded_at` para `fact_flight_position`).
- `--mode full`: sem filtro (backfill completo), exceto `fact_flight_position` que usa range
  máximo configurado (para não varrer todo o histórico sem necessidade).

## 7. Versionamento

O arquivo é versionado no repositório (`app/src/dependencies/config/tables_config.json`)
e **promovido via Terraform** (`aws_s3_object`). Mudanças de schema exigem bump + sync
`silver_modeling.md` e `docs-sdd-emr/`.

---

**Fim do documento.** Salve como `tables_config_schema.md`.