SELECT
  id,
  supavisor_logs.timestamp,
  event_message,
  metadata.level,
  metadata.project,
  metadata.region
FROM supavisor_logs
CROSS JOIN unnest(metadata) AS metadata
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
