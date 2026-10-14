SELECT
  xxhash64(cast(r.id as string))            AS route_key,
  r.id                                      AS route_id,
  r.airline_iata,
  r.airline_id,
  xxhash64(o.icao_code)                     AS origin_airport_key,
  xxhash64(d.icao_code)                     AS destination_airport_key,
  o.icao_code                               AS origin_icao,
  d.icao_code                               AS destination_icao,
  r.src_airport                             AS origin_iata,
  r.dst_airport                             AS destination_iata,
  r.codeshare,
  r.stops,
  r.equipment,
  r.duration_minutes,
  current_timestamp()                       AS trusted_ingestion_ts,
  'EMR_SPARK'                               AS trusted_process_engine,
  date(coalesce(r.updated_at, r.created_at)) AS event_date
FROM db_raw.fr_routes r
LEFT JOIN db_raw.fr_airports o
  ON r.src_airport_id = o.id
LEFT JOIN db_raw.fr_airports d
  ON r.dst_airport_id = d.id
WHERE r.cdc_operation <> 'D'
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY r.updated_at DESC NULLS LAST) = 1