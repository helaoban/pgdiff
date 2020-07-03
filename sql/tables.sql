WITH extension_oids AS (

  SELECT objid as oid
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass

), columns AS (

    SELECT
        a.attrelid as table_oid,
        a.attname AS name,
        format_type(a.atttypid, atttypmod) AS type,
        COALESCE(pg_get_expr(ad.adbin, ad.adrelid), 'NULL') AS default,
        a.attnotnull AS not_null
    FROM pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_attrdef ad
        ON a.attrelid = ad.adrelid
        AND a.attnum = ad.adnum
    WHERE a.attnum > 0
    AND a.attisdropped = FALSE

), constraints AS (

    SELECT
        ct.conrelid AS table_oid,
        ct.oid AS oid,
        n.nspname AS schema,
        ct.conname AS name,
	    format('%I.%I', n.nspname, ct.conname) AS identity,
        pg_get_constraintdef(ct.oid) AS definition,
	    format('%I.%I', n.nspname, ci.relname) AS index
    FROM pg_constraint ct
    INNER JOIN pg_catalog.pg_namespace n ON n.oid = ct.connamespace
    INNER JOIN pg_catalog.pg_index idx ON idx.indexrelid = ct.conindid
    INNER JOIN pg_catalog.pg_class ci ON ci.oid = idx.indexrelid

), table_attrs AS (

    SELECT
        c.oid AS oid,
        c.relname as name,
        n.nspname as schema,
        format('%I.%I', n.nspname, c.relname) AS identity,
        c.relkind AS type,
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
        pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
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

), table_aggs AS (

    SELECT
        t.oid as oid,
        json_agg(to_jsonb(col) - 'table_oid') as columns,
        json_agg(to_jsonb(con) - 'table_oid') as constraints
    FROM table_attrs t
    LEFT JOIN columns col on col.table_oid = t.oid
    LEFT OUTER JOIN constraints con ON con.table_oid = t.oid
    GROUP BY t.oid
)
SELECT
    t.*,
    ta.columns,
    ta.constraints
FROM table_attrs t
INNER JOIN table_aggs ta on ta.oid = t.oid
