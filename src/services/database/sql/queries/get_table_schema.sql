WITH pk AS (
    SELECT ccu.column_name
    FROM information_schema.table_constraints AS tc
    INNER JOIN information_schema.constraint_column_usage AS ccu
        ON tc.constraint_name = ccu.constraint_name
    WHERE
        tc.table_schema = '{schema_name}'
        AND tc.table_name = '{table}'
        AND tc.constraint_type = 'PRIMARY KEY'
),

fk AS (
    SELECT
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    INNER JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    INNER JOIN information_schema.constraint_column_usage AS ccu
        ON tc.constraint_name = ccu.constraint_name
    WHERE
        tc.table_schema = '{schema_name}'
        AND tc.table_name = '{table}'
        AND tc.constraint_type = 'FOREIGN KEY'
)

SELECT DISTINCT
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.ordinal_position,
    fk.foreign_table_name,
    fk.foreign_column_name,
    col_description(pc.oid, c.ordinal_position) AS column_description,
    coalesce(pk.column_name IS NOT NULL, FALSE) AS is_primary_key
FROM information_schema.columns AS c
INNER JOIN pg_class AS pc
    ON
        pc.relname = '{table}'
        AND pc.relnamespace = (
            SELECT oid FROM pg_namespace
            WHERE nspname = '{schema_name}'
        )
LEFT JOIN pk ON c.column_name = pk.column_name
LEFT JOIN fk ON c.column_name = fk.column_name
WHERE
    c.table_schema = '{schema_name}'
    AND c.table_name = '{table}'
ORDER BY c.ordinal_position;
