SELECT
  id,
  postgres_logs.timestamp,
  event_message,
  identifier,
  parsed.error_severity,
  parsed.query,
  parsed.application_name
FROM postgres_logs
CROSS JOIN unnest(metadata) AS m
CROSS JOIN unnest(m.parsed) AS parsed
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
