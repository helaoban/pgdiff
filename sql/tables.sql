WITH extension_oids AS (
  SELECT objid as oid
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass
), tables AS (
    SELECT
        c.oid AS oid,
        n.nspname as schema,
        array_agg(a.attname) as columns,
        array_agg(format_type(a.atttypid, atttypmod)) AS column_types,
        array_agg(COALESCE(pg_get_expr(ad.adbin, ad.adrelid), 'NULL')) as column_defaults,
        array_agg(a.attnotnull) as not_null_columns
    FROM
        pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attribute a
            ON a.attrelid = c.oid 
            AND a.attnum > 0
            AND a.attisdropped = FALSE
        LEFT JOIN pg_catalog.pg_attrdef ad
            ON a.attrelid = ad.adrelid
            AND a.attnum = ad.adnum
        LEFT OUTER JOIN extension_oids e ON c.oid = e.oid
    WHERE c.relkind in ('r', 'p')
    -- INTERNAL 
    AND e.oid IS null
    -- INTERNAL 
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_temp_%' 
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_toast_temp_%'
    GROUP BY c.oid, n.nspname
), table_constraints AS (
    SELECT
        t.oid AS oid,
        array_agg(pcn.conname) AS constraints,
        array_agg(pg_get_constraintdef(pcn.oid)) AS constraint_definitions
    FROM tables t
    LEFT OUTER JOIN pg_constraint pcn ON pcn.conrelid = t.oid
    GROUP BY t.oid
)
SELECT
    t.oid,
    t.schema as schema,
    c.relname AS name,
    c.relkind AS type,
    t.columns as columns,
    t.column_types as column_types,
    t.column_defaults as column_defaults,
    t.not_null_columns as not_null_columns,
    tc.constraints as constraints,
    tc.constraint_definitions as constraint_definitions,
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
    c.relpersistence AS persistence
FROM 
    tables t
    INNER JOIN pg_catalog.pg_class c ON c.oid = t.oid
    INNER JOIN table_constraints tc ON tc.oid = t.oid;
