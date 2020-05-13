WITH things1 AS (

  SELECT
    p.oid,
    n.nspname AS schema,
    p.proname AS name,
    format(
        '%I.%I(%s)',
        n.nspname,
        p.proname,
        pg_get_function_identity_arguments(p.oid)
    ) AS identity,
    'f' AS kind
  FROM pg_proc p
  INNER JOIN pg_namespace n ON p.pronamespace = n.oid
  -- 11_AND_LATER
  WHERE p.prokind != 'a'
  -- INTERNAL
  AND nspname not IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
  -- INTERNAL
  AND nspname not like 'pg_temp_%' 
  -- INTERNAL
  AND nspname not like 'pg_toast_temp_%'

  UNION

  SELECT
    c.oid,
    n.nspname as schema,
    c.relname AS name,
    format('%I.%I', n.nspname, c.relname) as identity,
    c.relkind AS kind
  FROM pg_class c
  INNER JOIN pg_namespace n ON c.relnamespace = n.oid
  WHERE c.oid not IN (SELECT ftrelid FROM pg_foreign_table)
  -- INTERNAL
  AND nspname not IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
  -- INTERNAL
  AND nspname not like 'pg_temp_%' 
  -- INTERNAL
  AND nspname not like 'pg_toast_temp_%'

), extensions AS (

  SELECT objid AS oid
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass

), things AS (

    SELECT
      t.oid,
      t.kind,
      t.schema,
      t.name,
      t.identity
    FROM things1 t
    LEFT OUTER JOIN extensions e ON t.oid = e.oid
    WHERE kind IN ('r', 'v', 'm', 'c', 'f') 
      -- OMIT EXTENSIONS
      AND e.oid is null

), combined AS (

  SELECT distinct
    t.oid,
    t.schema,
    t.name,
    t.identity,
    deps.oid AS dependency_oid,
    deps.schema AS dependency_schema,
    deps.name AS dependency_name,
    deps.identity AS dependency_identity
  FROM
      pg_depend d
      INNER JOIN things deps ON d.refobjid = deps.oid
      INNER JOIN pg_rewrite rw
        ON d.objid = rw.oid
        AND deps.oid != rw.ev_class
      INNER JOIN things t ON rw.ev_class = t.oid
  WHERE
    d.deptype in ('n')
    AND rw.rulename = '_RETURN'

)
SELECT * FROM combined;
