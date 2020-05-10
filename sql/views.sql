WITH extension_oids AS (
  SELECT objid as oid
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass
)
SELECT
    c.oid AS oid,
    n.nspname AS schema,
    c.relname AS name,
    c.relkind AS type,
    pg_get_viewdef(c.oid) as definition
FROM
    pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT OUTER JOIN extension_oids e ON c.oid = e.oid
WHERE c.relkind in ('v', 'm')
-- INTERNAL 
AND e.oid IS null
-- INTERNAL 
AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
-- INTERNAL 
AND n.nspname NOT LIKE 'pg_temp_%' 
-- INTERNAL 
AND n.nspname NOT LIKE 'pg_toast_temp_%'
