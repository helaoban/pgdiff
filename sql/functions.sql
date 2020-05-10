WITH r1 AS (
    SELECT
        r.routine_name AS name,
        r.routine_schema AS schema,
        r.specific_name AS specific_name,
        r.data_type,
        r.type_udt_schema,
        r.type_udt_name,
        r.external_language,
        r.routine_definition AS definition
    FROM information_schema.routines r
    -- INTERNAL 
    WHERE r.external_language NOT IN ('C', 'INTERNAL')
    -- INTERNAL 
    AND r.routine_schema NOT IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- INTERNAL 
    AND r.routine_schema NOT LIKE 'pg_temp_%' 
    -- INTERNAL 
    AND r.routine_schema NOT LIKE 'pg_toast_temp_%'
    ORDER BY
        r.specific_name
), pgproc AS (
    SELECT
    n.nspname AS schema,
    proname AS name,
    p.oid AS oid,
    CASE proisstrict WHEN true THEN
      'RETURNS NULL ON NULL INPUT'
    ELSE
      'CALLED ON NULL INPUT'
    END AS strictness,
    CASE prosecdef WHEN true THEN
      'SECURITY DEFINER'
    ELSE
      'SECURITY INVOKER'
    END AS security_type,
    CASE provolatile
      WHEN 'i' THEN
        'IMMUTABLE'
      WHEN 's' THEN
        'STABLE'
      WHEN 'v' THEN
        'VOLATILE'
      ELSE
        null
    END AS volatility,
    p.prokind AS kind
    -- 10_AND_EARLIER CASE WHEN p.proisagg THEN 'a' ELSE 'f' END AS kind
    FROM pg_proc p
    INNER JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE true
    AND p.prokind != 'a'
    -- INTERNAL 
    AND n.nspname NOT IN ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_temp_%' 
    -- INTERNAL 
    AND n.nspname NOT LIKE 'pg_toast_temp_%'
), extension_oids AS (
  SELECT objid FROM pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
      AND d.classid = 'pg_proc'::regclass
), r AS (
    SELECT
        r1.*,
        pp.volatility,
        pp.strictness,
        pp.security_type,
        pp.oid,
        pp.kind,
        e.objid AS extension_oid
    FROM r1
    LEFT OUTER JOIN pgproc pp
      ON r1.schema = pp.schema
      AND r1.specific_name = pp.name || '_' || pp.oid
    LEFT OUTER JOIN extension_oids e ON pp.oid = e.objid
    -- SKIP_INTERNAL 
    WHERE e.objid is null
), pre AS (
    SELECT
        r.schema AS schema,
        r.name AS name,
        CASE WHEN r.data_type = 'USER-DEFINED' THEN
          '"' || r.type_udt_schema || '"."' || r.type_udt_name || '"'
        ELSE
          r.data_type
        END AS return_type,
        r.data_type = 'USER-DEFINED' AS has_user_defined_return_type,
        p.parameter_name AS parameter_name,
        p.data_type AS data_type,
        p.parameter_mode AS parameter_mode,
        p.parameter_default AS parameter_default,
        p.ordinal_position AS position_number,
        r.definition AS definition,
        pg_get_functiondef(oid) AS full_definition,
        r.external_language AS language,
        r.strictness AS strictness,
        r.security_type AS security_type,
        r.volatility AS volatility,
        r.kind AS kind,
        r.oid AS oid,
        r.extension_oid AS extension_oid,
        pg_get_function_result(oid) AS result_string,
        pg_get_function_identity_arguments(oid) AS identity_arguments,
        pg_catalog.obj_description(r.oid) AS comment
    FROM r
    LEFT JOIN information_schema.parameters p 
        ON r.specific_name=p.specific_name
    ORDER BY
        name, parameter_mode, ordinal_position, parameter_name
)
SELECT * FROM pre
ORDER BY
    schema,
    name,
    parameter_mode,
    position_number,
    parameter_name;
