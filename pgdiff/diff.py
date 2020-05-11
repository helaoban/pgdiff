import os
import sys
import typing as t
from . import objects as obj, helpers


if t.TYPE_CHECKING:
    from .objects import Table, DBObject, Index, View, Sequence, Enum, Function
    from .objects import DatabaseDiff
    Column = t.Tuple[str, str, str, bool]
    Constraint = t.Tuple[str, str]


SQL_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        "sql",
    )
)

TABLE_QUERY = os.path.join(SQL_DIR, "tables.sql")
VIEW_QUERY = os.path.join(SQL_DIR, "views.sql")
INDEX_QUERY = os.path.join(SQL_DIR, "indices.sql")
SEQUENCE_QUERY = os.path.join(SQL_DIR, "sequences.sql")
ENUM_QUERY = os.path.join(SQL_DIR, "enums.sql")
FUNCTION_QUERY = os.path.join(SQL_DIR, "functions2.sql")
TRIGGER_QUERY = os.path.join(SQL_DIR, "triggers.sql")

T = t.TypeVar("T")
IT = t.TypeVar("IT", bound=obj.DBObject)

def query(cur, query, type_: str) -> t.List[dict]:
    with open(query, "r") as f:
        sql = f.read()
    cur.execute(sql)
    results = []
    for record in cur:
        result = dict(**{"obj_type": type_, **record})
        results.append(result)
    return results


def _index_by_id(items: t.List[IT]) -> t.Dict[str, IT]:
    rv = {}
    for x in items:
        rv[helpers.get_obj_id(x)] = x
    return rv


def inspect(cur) -> obj.Database:
    tables = query(cur, TABLE_QUERY, "table")  # type: t.List[obj.Table]
    views = query(cur, VIEW_QUERY, "view")  # type: t.List[obj.View]
    indices = query(cur, INDEX_QUERY, "index")  # type: t.List[obj.Index]
    sequences = query(cur, SEQUENCE_QUERY, "sequence")  # type: t.List[obj.Sequence]
    enums = query(cur, ENUM_QUERY, "enum")  # type: t.List[obj.Enum]
    functions = query(cur, FUNCTION_QUERY, "function")  # type: t.List[obj.Function]
    triggers = query(cur, TRIGGER_QUERY, "trigger")  # type: t.List[obj.Trigger]
    return dict(
        tables=_index_by_id(tables),
        views=_index_by_id(views),
        indices=_index_by_id(indices),
        enums=_index_by_id(enums),
        sequences=_index_by_id(sequences),
        functions=_index_by_id(functions),
        triggers=_index_by_id(triggers),
    )


def diff_identifiers(
    source: t.Set[str],
    target: t.Set[str],
) -> obj.DatabaseIdDiff:
    common = source & target
    unique_to_source = source - target
    unique_to_target = target - source
    return common, unique_to_source, unique_to_target


def diff_index(source: "Index", target: "Index") -> t.List[str]:
    return []


def diff_view(source: "View", target: "View") -> t.Optional[str]:
    if source["definition"] != target["definition"]:
        return (
            "CREATE OR REPLACE VIEW %s\n" % target["name"]
        ) + target["definition"]
    return None


def diff_column(source: "Column", target: "Column") -> t.List[str]:
    rv = []
    sname, stype, sdefault, snotnull = source
    tname, ttype, tdefault, tnotnull = target

    if stype != ttype:
        change = "ALTER COLUMN TYPE %s" % ttype
        rv.append(change)

    if sdefault != tdefault:
        if tdefault is None:
            change = "ALTER COLUMN DROP DEFAULT"
        else:
            change = "ALTER COLUMN SET DEFAULT %s" % tdefault
        rv.append(change)

    if snotnull != tnotnull:
        if tnotnull is True:
            change = "ALTER COLUMN SET NOT NULL"
        else:
            change = "ALTER COLUMN SDROP NOT NULL"
        rv.append(change)

    return rv


def diff_columns(source: "Table", target: "Table") -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["columns"]), set(target["columns"]))

    for col_name in common:
        source_col = helpers.get_column(source, col_name)
        target_col = helpers.get_column(target, col_name)
        rv.extend(diff_column(source_col, target_col))

    for col_name in source_unique:
        rv.append("DROP COLUMN %s" % col_name)

    for col_name in target_unique:
        col = helpers.get_column(target, col_name)
        rv.append(helpers.make_column_add(col))
    return rv


def diff_constraints(source: "Table", target: "Table") -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["constraints"]), set(target["constraints"]))
    for constraint_name in source_unique:
        drop = "DROP CONSTRAINT %s" % constraint_name
        rv.append(drop)
    for constraint_name in target_unique:
        _, definition = helpers.get_constraint(target, constraint_name)
        add = "ADD %s %s" % (constraint_name, definition)
        rv.append(add)
    for constraint_name in common:
        _, source_definition = helpers.get_constraint(source, constraint_name)
        _, target_definition = helpers.get_constraint(target, constraint_name)
        if source_definition != target_definition:
            drop = "DROP CONSTRAINT %s" % constraint_name
            add = "ADD %s %s" % (constraint_name, target_definition)
            rv.extend([drop, add])
    return rv


def diff_table(source: "Table", target: "Table") -> t.Optional[str]:
    alterations = []
    alterations.extend(diff_columns(source, target))
    alterations.extend(diff_constraints(source, target))
    if alterations:
        return "ALTER TABLE {name} {alterations}".format(
            name=target["name"],
            alterations=" ".join(alterations),
        )
    return None


def diff_triggers(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["triggers"]), set(target["triggers"]))
    for trigger_id in source_unique:
        drop = "DROP TRIGGER %s" % trigger_id
        rv.append(drop)
    for trigger_id in target_unique:
        target_trigger = target["triggers"][trigger_id]
        rv.append(target_trigger["definition"])
    for trigger_id in common:
        source_trigger = source["triggers"][trigger_id]
        target_trigger = target["triggers"][trigger_id]
        if source_trigger["definition"] != target_trigger["definition"]:
            drop = "DROP TRIGGER %s" % trigger_id
            rv.extend([drop, target_trigger["definition"]])
    return rv


def diff_function(source: "Function", target: "Function") -> t.Optional[str]:
    if source["definition"] != target["definition"]:
        # TODO definition needs to be CREATE OR REPLACE
        return target["definition"]
    return None


def diff_functions(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["functions"]), set(target["functions"]))
    for function_id in source_unique:
        source_function = source["functions"][function_id]
        drop = "DROP FUNCTION %s" % source_function["signature"]
        rv.append(drop)
    for function_id in target_unique:
        target_function = target["functions"][function_id]
        rv.append(target_function["definition"])
    for function_id in common:
        source_function = source["functions"][function_id]
        target_function = target["functions"][function_id]
        diff = diff_function(source_function, target_function)
        if diff:
            rv.append(diff)
    return rv


def diff_enum(source: "Enum", target: "Enum") -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["elements"]), set(target["elements"]))
    if source_unique:
        drop = "DROP TYPE %s" % source["name"]
        create = helpers.make_enum_create(target)
        rv.extend([drop, create])
        return rv

    for el in target_unique:
        alter = "ALTER TYPE %s ADD VALUE '%s'" % (target["name"], el)
        rv.append(alter)

    return rv


def diff_enums(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["enums"]), set(target["enums"]))
    for enum_id in source_unique:
        rv.append("DROP TYPE %s" % enum_id)
    for enum_id in target_unique:
        target_enum = target["enums"][enum_id]
        rv.append(helpers.make_enum_create(target_enum))
    for enum_id in common:
        source_enum = source["enums"][enum_id]
        target_enum = target["enums"][enum_id]
        rv.extend(diff_enum(source_enum, target_enum))
    return rv


def diff_sequences(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv =  []
    common, source_unique, target_unique = diff_identifiers(
        set(source["sequences"]), set(target["sequences"]))

    for sequence_id in source_unique:
        rv.append("DROP SEQUENCE %s" % sequence_id)

    for sequence_id in target_unique:
        target_sequence = target["sequences"][sequence_id]
        rv.append(helpers.make_sequence_create(target_sequence))

    return rv


def diff_indices(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []

    common, source_unique, target_unique = diff_identifiers(
        set(source["indices"]), set(target["indices"]))

    for index_id in source_unique:
        rv.append("DROP INDEX %s" % index_id)

    for index_id in target_unique:
        target_index = target["indices"][index_id]
        rv.append(target_index["definition"])

    for index_id in common:
        source_index = source["indices"][index_id]
        target_index = target["indices"][index_id]
        index_diff = diff_index(source_index, target_index)
        if index_diff:
            rv.extend(index_diff)

    return rv


def diff_views(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []

    common, source_unique, target_unique = diff_identifiers(
        set(source["views"]), set(target["views"]))

    for view_id in source_unique:
        rv.append("DROP VIEW %s" % view_id)

    for view_id in target_unique:
        target_view = target["views"][view_id]
        statement = (
            "CREATE VIEW %s\n" % target_view["name"]
        ) + target_view["definition"]
        rv.append(statement)

    for view_id in common:
        source_view = source["views"][view_id]
        target_view = target["views"][view_id]
        view_diff = diff_view(source_view, target_view)
        if view_diff:
            rv.append(view_diff)

    return rv


def diff_tables(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []

    common, source_unique, target_unique = diff_identifiers(
        set(source["tables"]), set(target["tables"]))

    for table_id in source_unique:
        rv.append("DROP TABLE %s" % table_id)

    for table_id in target_unique:
        target_table = target["tables"][table_id]
        rv.append(helpers.make_table_create(target_table))

    for table_id in common:
        source_table = source["tables"][table_id]
        target_table = target["tables"][table_id]
        table_diff = diff_table(source_table, target_table)
        if table_diff:
            rv.append(table_diff)

    return rv


def diff(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    rv.extend(diff_tables(source, target))
    rv.extend(diff_views(source, target))
    rv.extend(diff_indices(source, target))
    rv.extend(diff_sequences(source, target))
    rv.extend(diff_enums(source, target))
    rv.extend(diff_functions(source, target))
    rv.extend(diff_triggers(source, target))
    return rv
