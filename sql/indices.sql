WITH extension_oids as (
    SELECT objid as oid
    FROM pg_depend d
    WHERE d.refclassid = 'pg_extension'::regclass
) SELECT 
    i.oid AS oid,
    n.nspname AS schema,
    c.relname AS table_name,
    i.relname AS name,
	format('%I.%I', n.nspname, i.relname) AS identity,
    pg_get_indexdef(i.oid) AS definition,
    (
        SELECT 
        string_agg(attname, ' ' ORDER BY attname) 
        FROM pg_attribute 
        WHERE attnum = any(string_to_array(x.indkey::text, ' ')::int[]) 
        AND attrelid = x.indrelid
    ) key_columns,
    indoption key_options, 
    indnatts num_columns, 
    indisunique is_unique,
    indisprimary is_pk, 
    indisexclusion is_exclusion, 
    indimmediate is_immediate,
    indisclustered is_clustered, 
    pg_get_expr(indexprs, indrelid) key_expressions,
    pg_get_expr(indpred, indrelid) partial_predicate
FROM pg_index x
    JOIN pg_class c ON c.oid = x.indrelid
    JOIN pg_class i ON i.oid = x.indexrelid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN extension_oids e ON c.oid = e.oid OR i.oid = e.oid
WHERE c.relkind IN ('r', 'm', 'p') AND i.relkind IN ('i', 'I')
-- INTERNAL 
AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
-- INTERNAL 
AND nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast_temp_%'
AND e.oid is null
ORDER BY 1, 2, 3;
