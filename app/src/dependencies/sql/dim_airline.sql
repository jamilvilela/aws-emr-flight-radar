SELECT
  xxhash64(icao_code)                      AS airline_key,
  icao_code,
  iata_code,
  name,
  alias,
  callsign,
  country,
  is_active,
  created_at,
  current_timestamp()                      AS trusted_ingestion_ts,
  'EMR_SPARK'                              AS trusted_process_engine,
  date(coalesce(updated_at, created_at))   AS event_date
FROM db_raw.fr_airlines
WHERE cdc_operation <> 'D'
QUALIFY ROW_NUMBER() OVER (PARTITION BY icao_code ORDER BY updated_at DESC NULLS LAST) = 1