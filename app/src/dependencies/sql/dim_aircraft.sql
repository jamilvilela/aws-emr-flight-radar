SELECT
  xxhash64(a.icao24)                        AS aircraft_key,
  a.icao24,
  a.registration,
  a.aircraft_type,
  at.iata_code                              AS type_iata_code,
  at.name                                   AS type_name,
  at.manufacturer,
  a.serial_number,
  a.operator_icao,
  a.operator_name,
  a.year_built,
  a.created_at,
  a.updated_at,
  current_timestamp()                       AS trusted_ingestion_ts,
  'EMR_SPARK'                               AS trusted_process_engine,
  date(coalesce(a.updated_at, a.created_at)) AS event_date
FROM db_raw.fr_aircraft a
LEFT JOIN db_raw.fr_aircraft_types at
  ON a.aircraft_type = at.icao_code
WHERE a.cdc_operation <> 'D'
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.icao24 ORDER BY a.updated_at DESC NULLS LAST) = 1