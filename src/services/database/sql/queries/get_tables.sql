(
-- Regular tables & views: full metadata available
    SELECT
        t.table_name,
        t.table_type,
        obj_description(pc.oid) AS description,
        pg_total_relation_size(format('%I.%I', t.table_schema, t.table_name))::bigint AS size_bytes,
        pg_stat_get_live_tuples(pc.oid)::bigint AS row_count,
        (
            SELECT count(*) FROM information_schema.columns AS c
            WHERE
                c.table_schema = t.table_schema
                AND c.table_name = t.table_name
        ) AS column_count,
        (
            SELECT count(*) FROM pg_indexes AS i
            WHERE
                i.schemaname = t.table_schema
                AND i.tablename = t.table_name
        ) AS index_count
    FROM information_schema.tables AS t
    INNER JOIN pg_class AS pc
        ON
            t.table_name = pc.relname
            AND pc.relnamespace = (
                SELECT oid FROM pg_namespace
                WHERE nspname = '{schema_name}'
            )
    WHERE
        t.table_schema = '{schema_name}'
        AND t.table_type IN ('BASE TABLE', 'VIEW')
)
UNION ALL
(
-- Foreign tables: limited metadata (size & row count functions don't apply)
    SELECT
        ft.foreign_table_name AS table_name,
        'FOREIGN TABLE' AS table_type,
        (
            SELECT obj_description(
                (quote_ident(ft.foreign_table_schema) || '.' || quote_ident(ft.foreign_table_name))::regclass
            )
        ) AS description,
        0::bigint AS size_bytes,
        NULL::bigint AS row_count,
        (
            SELECT count(*) FROM information_schema.columns AS c
            WHERE
                c.table_schema = ft.foreign_table_schema
                AND c.table_name = ft.foreign_table_name
        ) AS column_count,
        0 AS index_count
    FROM information_schema.foreign_tables AS ft
    WHERE ft.foreign_table_schema = '{schema_name}'
)
ORDER BY size_bytes DESC;
