WITH extension_oids AS (
  SELECT objid AS oid FROM pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
      AND d.classid = 'pg_proc'::regclass
)
SELECT
    tg.oid AS oid,
    nsp.nspname AS "schema",
    tg.tgname  AS "name",
	format('%I.%I', nsp.nspname, tg.tgname) AS identity,
    cls.relname AS table_name,
    pg_get_triggerdef(tg.oid) AS definition,
    proc.proname AS proc_name,
    nspp.nspname AS proc_schema,
    tg.tgenabled AS enabled
FROM pg_trigger tg
    INNER JOIN pg_class cls ON cls.oid = tg.tgrelid
    INNER JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
    INNER JOIN pg_proc proc ON proc.oid = tg.tgfoid
    INNER JOIN pg_namespace nspp ON nspp.oid = proc.pronamespace
    LEFT OUTER JOIN extension_oids e ON e.oid = proc.oid
WHERE NOT tg.tgisinternal
AND e.oid is null
ORDER BY schema, table_name, name;
