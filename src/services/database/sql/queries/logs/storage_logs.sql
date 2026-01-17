SELECT
  id,
  storage_logs.timestamp,
  event_message,
  metadata.level,
  metadata.project,
  metadata.responseTime,
  metadata.rawError
FROM storage_logs
CROSS JOIN unnest(metadata) AS metadata
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
