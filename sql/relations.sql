WITH extension_oids AS (
  SELECT objid as oid
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass
), relations AS (
    SELECT
        e.oid as eoid,
        c.oid AS oid,
        n.nspname AS schema,
        c.relname AS name,
        c.relkind AS type,
        (
            CASE WHEN c.relkind in ('m', 'v') THEN
              pg_get_viewdef(c.oid)
            ELSE null END
        ) AS definition,
        (
          SELECT
              '"' || nmsp_parent.nspname || '"."' || parent.relname || '"' AS parent
          FROM pg_inherits
              JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
              JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
              JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
              JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
          WHERE child.oid = c.oid
        ) AS parent_table,
        (
            CASE WHEN c.relpartbound IS NOT null THEN
              pg_get_expr(c.relpartbound, c.oid, true)
            WHEN c.relhassubclass IS NOT null THEN
              pg_catalog.pg_get_partkeydef(c.oid)
            END
        ) AS partition_def,
        c.relrowsecurity::boolean AS row_security,
        c.relforcerowsecurity::boolean AS force_row_security,
        c.relpersistence AS persistence,
        c.relpages AS page_size_estimate,
        c.reltuples AS row_count_estimate
    FROM
        pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT OUTER JOIN extension_oids e ON c.oid = e.oid
    WHERE c.relkind in ('r', 'v', 'm', 'c', 'p')
    -- INTERNAL 
    AND e.oid IS null
    -- INTERNAL 
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_temp_%' 
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_toast_temp_%'

)
SELECT
    r.*
FROM
    relations r
-- INTERNAL 
WHERE r.schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
-- INTERNAL 
AND r.schema NOT LIKE 'pg_temp_%' 
-- INTERNAL 
AND r.schema NOT LIKE 'pg_toast_temp_%'
ORDER BY 
    r.type,
    r.schema,
    r.name
LIMIT 1;
