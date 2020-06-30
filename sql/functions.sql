WITH extension_oids AS (
  SELECT objid AS oid FROM pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
      AND d.classid = 'pg_proc'::regclass
)
SELECT
    pp.oid AS oid,
    n.nspname AS schema,
    pp.proname AS name,
    format(
        '%I(%s)',
        pp.proname,
        pg_get_function_identity_arguments(pp.oid)
    ) AS signature,
	format(
        '%I.%I', 
        n.nspname, 
        format(
            '%I(%s)',
            pp.proname,
            pg_get_function_identity_arguments(pp.oid)
        )
    ) AS identity,
    pl.lanname AS language,
    pp.proisstrict AS is_strict,
    pp.prosecdef AS is_security_definer,
    pp.provolatile AS volatility,
    pp.prokind AS kind,
    pp.proargnames AS argnames,
    COALESCE(
        pp.proallargtypes::regtype[], pp.proargtypes::regtype[]
    )::text[] AS argtypes,
    pp.prorettype::regtype::text return_type,
    pg_get_functiondef(pp.oid) AS definition
FROM pg_proc pp
INNER JOIN pg_namespace n ON n.oid = pp.pronamespace
INNER JOIN pg_language pl ON pl.oid = pp.prolang
LEFT OUTER JOIN extension_oids e ON e.oid = pp.oid
WHERE e.oid is null
AND pp.prokind != 'a'
-- INTERNAL 
AND n.nspname NOT IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- INTERNAL 
AND n.nspname NOT LIKE 'pg_temp_%' 
-- INTERNAL 
AND n.nspname NOT LIKE 'pg_toast_temp_%';
