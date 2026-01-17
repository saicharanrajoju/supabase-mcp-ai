SELECT
  id,
  function_edge_logs.timestamp,
  event_message,
  m.deployment_id,
  m.execution_time_ms,
  m.function_id,
  m.project_ref,
  request.method,
  request.pathname,
  request.url,
  request.host,
  response.status_code,
  m.version
FROM function_edge_logs
CROSS JOIN unnest(metadata) AS m
CROSS JOIN unnest(m.request) AS request
CROSS JOIN unnest(m.response) AS response
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
