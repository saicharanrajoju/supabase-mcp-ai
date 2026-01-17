SELECT
  id,
  auth_logs.timestamp,
  event_message,
  metadata.level,
  metadata.status,
  metadata.path,
  metadata.msg
FROM auth_logs
CROSS JOIN unnest(metadata) AS metadata
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
