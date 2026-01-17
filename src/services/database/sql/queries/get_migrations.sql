SELECT
    version,
    name,
    CASE
        WHEN '{include_full_queries}' = 'true' THEN statements
        ELSE NULL
    END AS statements,
    array_length(statements, 1) AS statement_count,
    CASE
        WHEN version ~ '^[0-9]+$' THEN 'numbered'
        ELSE 'named'
    END AS version_type
FROM supabase_migrations.schema_migrations
WHERE
    -- Filter by name if provided
    ('{name_pattern}' = '' OR name ILIKE '%' || '{name_pattern}' || '%')
ORDER BY
    -- Order by version (timestamp) descending
    version DESC
LIMIT {limit} OFFSET {offset};
