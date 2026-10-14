SELECT
  xxhash64(cast(id as string))              AS country_key,
  id,
  code,
  name,
  continent,
  wikipedia_link,
  current_timestamp()                       AS trusted_ingestion_ts,
  'EMR_SPARK'                               AS trusted_process_engine,
  date(coalesce(updated_at, created_at))    AS event_date
FROM db_raw.fr_countries
WHERE cdc_operation <> 'D'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC NULLS LAST) = 1