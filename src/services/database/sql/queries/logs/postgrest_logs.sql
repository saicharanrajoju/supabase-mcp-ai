SELECT
  id,
  postgrest_logs.timestamp,
  event_message,
  identifier,
  metadata.host
FROM postgrest_logs
CROSS JOIN unnest(metadata) AS metadata
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
