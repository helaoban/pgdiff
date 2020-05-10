SELECT
    nspname AS schema
FROM
    pg_catalog.pg_namespace
-- INTERNAL 
WHERE nspname NOT IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast') 
-- INTERNAL 
AND nspname NOT LIKE 'pg_temp_%' 
-- INTERNAL 
AND nspname NOT LIKE 'pg_toast_temp_%'
ORDER BY 1;
