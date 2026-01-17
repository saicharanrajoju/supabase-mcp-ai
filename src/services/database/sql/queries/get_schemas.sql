WITH t AS (
    -- Regular tables
    SELECT
        schemaname AS schema_name,
        tablename AS table_name,
        'regular' AS table_type
    FROM pg_tables

    UNION ALL

    -- Foreign tables
    SELECT
        foreign_table_schema AS schema_name,
        foreign_table_name AS table_name,
        'foreign' AS table_type
    FROM information_schema.foreign_tables
)

SELECT
    s.schema_name,
    COALESCE(PG_SIZE_PRETTY(SUM(
        COALESCE(
            CASE
                WHEN t.table_type = 'regular'
                    THEN PG_TOTAL_RELATION_SIZE(
                        QUOTE_IDENT(t.schema_name) || '.' || QUOTE_IDENT(t.table_name)
                    )
                ELSE 0
            END, 0
        )
    )), '0 B') AS total_size,
    COUNT(t.table_name) AS table_count
FROM information_schema.schemata AS s
LEFT JOIN t ON s.schema_name = t.schema_name
WHERE
    s.schema_name NOT IN ('pg_catalog', 'information_schema')
    AND s.schema_name NOT LIKE 'pg_%'
    AND s.schema_name NOT LIKE 'pg_toast%'
GROUP BY s.schema_name
ORDER BY
    COUNT(t.table_name) DESC,           -- Schemas with most tables first
    total_size DESC,                    -- Then by size
    s.schema_name ASC;                      -- Then alphabetically
