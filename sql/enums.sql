WITH extension_oids AS (

    SELECT objid AS oid 
    FROM pg_depend d
    WHERE d.refclassid = 'pg_extension'::regclass

), enums AS (

    SELECT 
        t.oid AS oid,
        n.nspname AS "schema",
        substr(
            pg_catalog.format_type(t.oid, NULL), 
            strpos(pg_catalog.format_type(t.oid, NULL), '.') + 1
        ) AS "name",
        ARRAY(
            SELECT e.enumlabel
            FROM pg_catalog.pg_enum e
            WHERE e.enumtypid = t.oid
            ORDER BY e.enumsortorder
        ) AS elements
    FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        LEFT OUTER JOIN extension_oids e ON t.oid = e.oid
    WHERE t.typcategory = 'E'
    AND e.oid is null
    -- INTERNAL 
    AND n.nspname NOT IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast_temp_%'
    ORDER BY 1, 2

)

SELECT
    e.oid,
    e.schema,
    e.name,
    e.elements,
	format('%I.%I', e.schema, e.name) AS identity
FROM enums e;
