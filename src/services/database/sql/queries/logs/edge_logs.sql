SELECT
  id,
  edge_logs.timestamp,
  event_message,
  identifier,
  request.method,
  request.path,
  response.status_code,
  request.url,
  response.origin_time
FROM edge_logs
CROSS JOIN unnest(metadata) AS m
CROSS JOIN unnest(m.request) AS request
CROSS JOIN unnest(m.response) AS response
{where_clause}
ORDER BY timestamp DESC
LIMIT {limit};
