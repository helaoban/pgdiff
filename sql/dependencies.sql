WITH extensions AS (

  SELECT objid AS oid
  FROM pg_depend d
  WHERE d.refclassid = 'pg_extension'::regclass

), composite_types AS (

	SELECT
		c.oid as oid,
		format('%I.%I', n.nspname, c.relname) as identity
	FROM pg_type t
	INNER JOIN pg_namespace n ON n.oid = t.typnamespace
	INNER JOIN pg_class c ON c.oid = t.typrelid
	LEFT OUTER JOIN extensions e on e.oid = c.oid
	WHERE t.typrelid <> 0
	AND c.relkind in ('r', 'v', 'm', 'c', 'p')
	AND e.oid IS null
	AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'

), enum_types AS (

	SELECT
		t.oid as oid,
		format('%I.%I', n.nspname, t.typname) as identity
	FROM pg_type t
	INNER JOIN pg_namespace n ON n.oid = t.typnamespace
	LEFT OUTER JOIN extensions e on e.oid = t.oid
	WHERE t.typcategory = 'E'
	AND e.oid IS null
	AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'

), combined_types AS (

	SELECT * FROM composite_types	
	UNION	
	SELECT * FROM enum_types

), functions AS (

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
        (
            CASE
                WHEN pt.typrelid <> 0 THEN pt.typrelid
                WHEN pt.typelem <> 0 THEN pt.typelem
                ELSE pt.oid
            END
        ) as rtype,
        p.proargtypes AS argtypes,
		'f' AS kind
	FROM pg_proc p
	INNER JOIN pg_namespace n ON p.pronamespace = n.oid
    INNER JOIN pg_type pt ON pt.oid = p.prorettype
    LEFT OUTER JOIN extensions e ON e.oid = p.oid
	-- 11_AND_LATER
	WHERE p.prokind <> 'a'
    AND e.oid IS null
	-- INTERNAL
    AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'

), function_return_types AS (

    SELECT
        f.oid,
        f.identity as identity,
        ct.oid as rtype_oid,
        ct.identity as rtype
    FROM functions f
    INNER JOIN combined_types ct ON ct.oid = f.rtype

), _function_arg_types_raw AS (

    SELECT 
        f.oid,
        UNNEST(f.argtypes) as argtype
    FROM functions f

), _function_arg_types AS (

    SELECT 
        f.oid,
        (
            CASE
                WHEN pt.typrelid <> 0 THEN pt.typrelid
                WHEN pt.typelem <> 0 THEN pt.typelem
                ELSE pt.oid
            END
        ) AS argtype
    FROM _function_arg_types_raw f
    INNER JOIN pg_type pt ON pt.oid = f.argtype

), function_arg_types AS (

    SELECT
        f.oid,
        f.identity,
        ct.oid as argtype_oid,
        ct.identity as argtype
    FROM _function_arg_types fa
    INNER JOIN functions f on f.oid = fa.oid
    INNER JOIN combined_types ct ON ct.oid = fa.argtype

), function_deps AS (

    SELECT
        f.oid as oid,
        f.identity as identity,
        f.argtype_oid as dependency_oid,
        f.argtype as dependency_indentity
    FROM function_arg_types f

    UNION

    SELECT
        f.oid as oid,
        f.identity as identity,
        f.rtype_oid as dependency_oid,
        f.rtype as dependency_indentity
    FROM function_return_types f

), relations AS (

	SELECT
		c.oid,
		n.nspname as schema,
		c.relname AS name,
		format('%I.%I', n.nspname, c.relname) as identity,
		c.relkind AS kind
	FROM pg_class c
	INNER JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE c.oid NOT IN (SELECT ftrelid FROM pg_foreign_table)
	-- INTERNAL
    AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'

), things1 AS (

    SELECT
        f.oid as oid,
        f.schema as schema,
        f.name as name,
        f.identity as identity,
        f.kind as kind
    FROM functions f
    UNION
    SELECT * FROM relations

), fk_deps AS (

	SELECT DISTINCT
		c.oid AS oid,
		format('%I.%I', n.nspname, c.relname) AS identity,
		deps.oid AS dependency_oid,
		format('%I.%I', dn.nspname, deps.relname) AS dependency_identity
	FROM pg_constraint pc
	INNER JOIN pg_class c ON c.oid = pc.conrelid
	INNER JOIN pg_class deps ON deps.oid = pc.confrelid
	INNER JOIN pg_namespace n ON n.oid=c.relnamespace
	INNER JOIN pg_namespace dn ON dn.oid=deps.relnamespace
	LEFT OUTER JOIN extensions ce ON ce.oid = c.oid
	LEFT OUTER JOIN extensions de ON de.oid = deps.oid
	WHERE pc.contype = 'f'
	-- INTERNAL
    AND n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
	-- INTERNAL
    AND dn.nspname NOT LIKE 'pg_%' AND dn.nspname <> 'information_schema'
	AND ce.oid IS NULL
	AND de.oid IS NULL

), trigger_table_deps AS (

    SELECT DISTINCT
		t.oid AS oid,
		format('%I.%I', n.nspname, t.tgname) AS identity,
        c.oid as dependency_oid,
		format('%I.%I', n.nspname, c.relname) AS dependency_identity
    FROM pg_trigger t
    INNER JOIN pg_class c ON c.oid = t.tgrelid
    INNER JOIN pg_namespace n ON n.oid = c.relnamespace
    INNER JOIN pg_proc p ON p.oid = t.tgfoid
    LEFT OUTER JOIN extensions e ON e.oid = p.oid
    WHERE NOT t.tgisinternal
    AND e.oid is null
	-- INTERNAL
	AND n.nspname NOT like 'pg_%' AND n.nspname <> 'information_schema'

), trigger_function_deps AS (

    SELECT DISTINCT
		t.oid AS oid,
		format('%I.%I', n.nspname, t.tgname) AS identity,
        p.oid as dependency_oid,
		format(
			'%I.%I(%s)',
			pn.nspname,
			p.proname,
			pg_get_function_identity_arguments(p.oid)
		) AS dependency_identity
    FROM pg_trigger t
    INNER JOIN pg_class c ON c.oid = t.tgrelid
    INNER JOIN pg_namespace n ON n.oid = c.relnamespace
    INNER JOIN pg_proc p ON p.oid = t.tgfoid
    INNER JOIN pg_namespace pn ON pn.oid = p.pronamespace
    LEFT OUTER JOIN extensions e ON e.oid = p.oid
    WHERE NOT t.tgisinternal
    AND e.oid is null
	-- INTERNAL
	AND n.nspname NOT like 'pg_%' AND n.nspname <> 'information_schema'

), trigger_deps AS (

    SELECT * FROM trigger_table_deps
    UNION
    SELECT * FROM trigger_function_deps

), index_deps AS (

    SELECT
        i.indexrelid as oid,
		format('%I.%I', n.nspname, c.relname) AS identity,
        i.indrelid as dependency_oid,
		format('%I.%I', dn.nspname, deps.relname) AS identity
    FROM pg_index i
    INNER JOIN pg_class c ON c.oid = i.indexrelid
    INNER JOIN pg_namespace n ON n.oid = c.relnamespace
    INNER JOIN pg_class deps ON deps.oid = i.indrelid
    INNER JOIN pg_namespace dn ON dn.oid = deps.relnamespace
    -- TODO needs to filter our extensions on deps as well?
    LEFT OUTER JOIN extensions e ON e.oid = c.oid
    LEFT OUTER JOIN extensions de ON de.oid = deps.oid
    WHERE e.oid is null
    AND de.oid is null
	-- INTERNAL
	AND n.nspname NOT like 'pg_%' AND n.nspname <> 'information_schema'
	AND dn.nspname NOT like 'pg_%' AND dn.nspname <> 'information_schema'
    
), things AS (

	SELECT
		t.oid,
		t.kind,
		t.schema,
		t.name,
		t.identity
	FROM things1 t
	LEFT OUTER JOIN extensions e ON t.oid = e.oid
	WHERE t.kind IN ('r', 'v', 'm', 'c', 'f', 'k')
	  -- OMIT EXTENSIONS
	AND e.oid is null

), combined AS (

	SELECT DISTINCT
		t.oid,
		t.identity,
		deps.oid AS dependency_oid,
		deps.identity AS dependency_identity
	FROM pg_depend d
		INNER JOIN things deps ON d.refobjid = deps.oid
		INNER JOIN pg_rewrite rw
			ON d.objid = rw.oid
			AND deps.oid != rw.ev_class
		INNER JOIN things t ON t.oid = rw.ev_class
	WHERE d.deptype in ('n')
	AND rw.rulename = '_RETURN'

	UNION

	SELECT * FROM fk_deps

    UNION

    SELECT * FROM function_deps

    UNION

    SELECT * FROM trigger_deps

    UNION

    SELECT * FROM index_deps

)
SELECT * FROM combined;
