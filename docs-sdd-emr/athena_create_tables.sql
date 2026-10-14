-- ============================================================
-- Athena CREATE EXTERNAL TABLE Statements for Trusted Layer
-- ============================================================
-- These statements register existing Delta Lake tables (created by EMR pipeline)
-- in Athena via Glue Data Catalog. Tables are partitioned by event_date.
-- Location paths follow the lakehouse pattern: s3://lakehouse-trusted-{account_id}/db_trusted/{table}/
--
-- Prerequisites:
-- 1. Tables already exist in Glue Catalog (created by infrastructure process)
-- 2. Delta Lake data exists in S3 at the specified locations
-- 3. Athena engine version 3+ with Delta Lake support
-- ============================================================

-- Set the database context
USE db_trusted;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- 2.1 dim_aircraft_type
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_aircraft_type (
  aircraft_type_key BIGINT,
  icao_code         STRING,
  iata_code         STRING,
  type_name         STRING,
  manufacturer      STRING,
  updated_at        TIMESTAMP,
  trusted_ingestion_ts TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_aircraft_type/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Aircraft type dimension - conformed from fr_aircraft_types',
  'layer'='trusted',
  'source'='fr_aircraft_types'
);

-- 2.2 dim_aircraft
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_aircraft (
  aircraft_key      BIGINT,
  icao24            STRING,
  registration      STRING,
  aircraft_type     STRING,
  type_iata_code    STRING,
  type_name         STRING,
  manufacturer      STRING,
  serial_number     STRING,
  operator_icao     STRING,
  operator_name     STRING,
  year_built        INT,
  created_at        TIMESTAMP,
  updated_at        TIMESTAMP,
  trusted_ingestion_ts TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_aircraft/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Aircraft dimension - conformed from fr_aircraft + fr_aircraft_types',
  'layer'='trusted',
  'source'='fr_aircraft, fr_aircraft_types'
);

-- 2.3 dim_airport
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_airport (
  airport_key       BIGINT,
  icao_code         STRING,
  iata_code         STRING,
  ident             STRING,
  type              STRING,
  name              STRING,
  latitude_deg      DECIMAL(10,6),
  longitude_deg     DECIMAL(10,6),
  elevation_ft      INT,
  continent         STRING,
  iso_country       STRING,
  iso_region        STRING,
  municipality      STRING,
  scheduled_service BOOLEAN,
  gps_code          STRING,
  local_code        STRING,
  home_link         STRING,
  wikipedia_link    STRING,
  trusted_ingestion_ts TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_airport/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Airport dimension - conformed from fr_airports',
  'layer'='trusted',
  'source'='fr_airports'
);

-- 2.4 dim_airline
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_airline (
  airline_key       BIGINT,
  icao_code         STRING,
  iata_code         STRING,
  name              STRING,
  alias             STRING,
  callsign          STRING,
  country           STRING,
  is_active         BOOLEAN,
  created_at        TIMESTAMP,
  trusted_ingestion_ts TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_airline/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Airline dimension - conformed from fr_airlines',
  'layer'='trusted',
  'source'='fr_airlines'
);

-- 2.5 dim_country
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_country (
  country_key       BIGINT,
  id                INT,
  code              STRING,
  name              STRING,
  continent         STRING,
  wikipedia_link    STRING,
  trusted_ingestion_ts TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_country/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Country dimension - conformed from fr_countries',
  'layer'='trusted',
  'source'='fr_countries'
);

-- 2.6 dim_route
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_route (
  route_key                 BIGINT,
  route_id                  BIGINT,
  airline_iata              STRING,
  airline_id                INT,
  origin_airport_key        BIGINT,
  destination_airport_key   BIGINT,
  origin_icao               STRING,
  destination_icao          STRING,
  origin_iata               STRING,
  destination_iata          STRING,
  codeshare                 STRING,
  stops                     INT,
  equipment                 STRING,
  duration_minutes          INT,
  trusted_ingestion_ts      TIMESTAMP,
  trusted_process_engine    STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_route/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Route dimension - conformed from fr_routes + fr_airports (origin/dest)',
  'layer'='trusted',
  'source'='fr_routes, fr_airports'
);

-- 2.7 dim_date (generated, not partitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.dim_date (
  date_key        INT,
  date            DATE,
  year            INT,
  quarter         INT,
  month           INT,
  month_name      STRING,
  day             INT,
  day_of_week     INT,
  day_name        STRING,
  week_of_year    INT,
  is_weekend      BOOLEAN,
  event_date      DATE
)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/dim_date/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Date dimension - generated calendar table',
  'layer'='trusted',
  'source'='generated'
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- 3.1 fact_flight
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.fact_flight (
  flight_id                BIGINT,
  flight_number            STRING,
  airline_key              BIGINT,
  aircraft_key             BIGINT,
  origin_airport_key       BIGINT,
  destination_airport_key  BIGINT,
  route_key                BIGINT,
  scheduled_departure      TIMESTAMP,
  scheduled_arrival        TIMESTAMP,
  actual_departure         TIMESTAMP,
  actual_arrival           TIMESTAMP,
  status                   STRING,
  source_ingestion_ts      TIMESTAMP,
  trusted_process_engine   STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/fact_flight/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Flight fact table - conformed from fr_flights with dimension keys',
  'layer'='trusted',
  'source'='fr_flights, dim_airline, dim_aircraft, dim_airport, dim_route'
);

-- 3.2 fact_flight_position
-- Partitioned by event_date = date(recorded_at)
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.fact_flight_position (
  position_id              BIGINT,
  recorded_at              TIMESTAMP,
  aircraft_icao24          STRING,
  aircraft_key             BIGINT,
  flight_id                BIGINT,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  altitude_ft              DOUBLE,
  velocity_kts             DOUBLE,
  heading                  DOUBLE,
  vertical_rate_fpm        DOUBLE,
  on_ground                BOOLEAN,
  ingested_at              TIMESTAMP,
  trusted_ingestion_ts     TIMESTAMP,
  trusted_process_engine   STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/fact_flight_position/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Flight position fact table - high volume, partitioned by recorded_at date',
  'layer'='trusted',
  'source'='fr_aircraft_positions, dim_aircraft'
);

-- 3.3 fact_flight_status (optional - SCD Type 2)
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.fact_flight_status (
  flight_id           BIGINT,
  status              STRING,
  status_change_ts    TIMESTAMP,
  effective_from      TIMESTAMP,
  effective_to        TIMESTAMP,
  is_current          BOOLEAN,
  trusted_ingestion_ts TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/fact_flight_status/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Flight status history - SCD Type 2 (optional)',
  'layer'='trusted',
  'source'='fr_flights (status changes)'
);

-- ============================================================
-- BRIDGE TABLES (optional - only if needed for Gold layer M:M)
-- ============================================================

-- bridge_flight_aircraft
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.bridge_flight_aircraft (
  flight_id              BIGINT,
  aircraft_key           BIGINT,
  icao24                 STRING,
  trusted_ingestion_ts   TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/bridge_flight_aircraft/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Bridge table: flight to aircraft (M:M if needed)',
  'layer'='trusted',
  'source'='fr_flights, dim_aircraft'
);

-- bridge_flight_route
CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.bridge_flight_route (
  flight_id              BIGINT,
  route_key              BIGINT,
  trusted_ingestion_ts   TIMESTAMP,
  trusted_process_engine STRING
)
PARTITIONED BY (event_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-trusted-${account_id}/db_trusted/bridge_flight_route/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Bridge table: flight to route (M:M if needed)',
  'layer'='trusted',
  'source'='fr_flights, dim_route'
);

-- ============================================================
-- SUPPORT TABLES (in db_raw - reused from Bronze)
-- ============================================================

USE db_raw;

-- etl_control
CREATE EXTERNAL TABLE IF NOT EXISTS db_raw.etl_control (
  execution_id        STRING,
  job_name            STRING,
  source              STRING,
  target_table        STRING,
  mode                STRING,
  execution_start     TIMESTAMP,
  execution_end       TIMESTAMP,
  status              STRING,
  records_read        BIGINT,
  records_written     BIGINT,
  records_rejected    BIGINT,
  error_message       STRING,
  reference_date      DATE
)
PARTITIONED BY (reference_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-raw-${account_id}/db_raw/etl_control/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='ETL execution control log',
  'layer'='bronze'
);

-- quality_metrics
CREATE EXTERNAL TABLE IF NOT EXISTS db_raw.quality_metrics (
  database            STRING,
  table               STRING,
  processing_timestamp TIMESTAMP,
  metric              STRING,
  rule                STRING,
  status              STRING,
  failure_reason      STRING,
  partition           STRING,
  technology          STRING,
  reference_date      DATE
)
PARTITIONED BY (reference_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-raw-${account_id}/db_raw/quality_metrics/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Data quality metrics',
  'layer'='bronze'
);

-- rejected_records
CREATE EXTERNAL TABLE IF NOT EXISTS db_raw.rejected_records (
  execution_id            STRING,
  execution_timestamp     TIMESTAMP,
  source_database         STRING,
  source_table            STRING,
  target_database         STRING,
  target_table            STRING,
  reject_rule             STRING,
  reject_reason           STRING,
  rejected_record_json    STRING,
  reference_date          DATE
)
PARTITIONED BY (reference_date DATE)
STORED AS DELTA
LOCATION 's3://lakehouse-raw-${account_id}/db_raw/rejected_records/'
TBLPROPERTIES (
  'delta.lastUpdateVersion'='0',
  'description'='Centralized rejected records from all ETL pipelines',
  'layer'='bronze'
);

-- ============================================================
-- MSCK REPAIR TABLE COMMANDS
-- ============================================================
-- Run after table creation to load partitions from S3
-- (Only needed if tables are created before data exists)

-- USE db_trusted;
-- MSCK REPAIR TABLE dim_aircraft_type;
-- MSCK REPAIR TABLE dim_aircraft;
-- MSCK REPAIR TABLE dim_airport;
-- MSCK REPAIR TABLE dim_airline;
-- MSCK REPAIR TABLE dim_country;
-- MSCK REPAIR TABLE dim_route;
-- MSCK REPAIR TABLE fact_flight;
-- MSCK REPAIR TABLE fact_flight_position;
-- MSCK REPAIR TABLE fact_flight_status;
-- MSCK REPAIR TABLE bridge_flight_aircraft;
-- MSCK REPAIR TABLE bridge_flight_route;

-- USE db_raw;
-- MSCK REPAIR TABLE etl_control;
-- MSCK REPAIR TABLE quality_metrics;
-- MSCK REPAIR TABLE rejected_records;

-- ============================================================
-- NOTES
-- ============================================================
-- 1. Replace ${account_id} with actual AWS account ID before execution
-- 2. Tables are registered in Glue Catalog - Athena reads from Glue
-- 3. Partition columns (event_date) are automatically discovered via MSCK REPAIR
-- 4. Delta Lake format requires Athena engine version 3+ 
-- 4. For incremental loads, new partitions are auto-discovered or use ALTER TABLE ADD PARTITION
-- 5. The 'created_at' column in dim_aircraft maps to a.updated_at from source
-- 6. Bridge tables are optional - only create if Gold layer requires M:M relationships