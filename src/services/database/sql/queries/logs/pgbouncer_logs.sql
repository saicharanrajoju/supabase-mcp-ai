SELECT
  id,
  pgbouncer_logs.timestamp,
  event_message,
  metadata.host,
  metadata.project
FROM pgbouncer_logs
CROSS JOIN unnest(metadata) AS metadata
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
