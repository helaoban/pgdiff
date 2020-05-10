SELECT
    tg.tgname  AS "name",
    nsp.nspname AS "schema",
    cls.relname AS table_name,
    pg_get_triggerdef(tg.oid) AS definition,
    proc.proname AS proc_name,
    nspp.nspname AS proc_schema,
    tg.tgenabled AS enabled
FROM pg_trigger tg
JOIN pg_class cls ON cls.oid = tg.tgrelid
JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
JOIN pg_proc proc ON proc.oid = tg.tgfoid
JOIN pg_namespace nspp ON nspp.oid = proc.pronamespace
WHERE NOT tg.tgisinternal
ORDER BY schema, table_name, name;
