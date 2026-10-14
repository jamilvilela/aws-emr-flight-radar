SELECT
  xxhash64(coalesce(icao_code, cast(id as string))) AS airport_key,
  icao_code,
  iata_code,
  ident,
  type,
  name,
  latitude_deg,
  longitude_deg,
  elevation_ft,
  continent,
  iso_country,
  iso_region,
  municipality,
  scheduled_service,
  gps_code,
  local_code,
  home_link,
  wikipedia_link,
  current_timestamp()                       AS trusted_ingestion_ts,
  'EMR_SPARK'                               AS trusted_process_engine,
  date(coalesce(updated_at, created_at))    AS event_date
FROM db_raw.fr_airports
WHERE cdc_operation <> 'D'
QUALIFY ROW_NUMBER() OVER (PARTITION BY coalesce(icao_code, cast(id as string)) ORDER BY updated_at DESC NULLS LAST) = 1