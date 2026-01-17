SELECT
  id,
  realtime_logs.timestamp,
  event_message,
  metadata.level,
  measurements.connected,
  measurements.connected_cluster,
  measurements.limit,
  measurements.sum,
  metadata.external_id
FROM realtime_logs
CROSS JOIN unnest(metadata) AS metadata
CROSS JOIN unnest(metadata.measurements) AS measurements
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
