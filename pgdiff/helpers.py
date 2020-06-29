import os
import typing as t
import typing_extensions as te

import networkx as nx  # type: ignore

from . import objects as obj


if t.TYPE_CHECKING:
    DBObjectType = t.Union[
        te.Literal["table"],
        te.Literal["view"],
        te.Literal["index"],
        te.Literal["sequence"],
        te.Literal["enum"],
        te.Literal["function"],
        te.Literal["trigger"],
        te.Literal["dependency"],
    ]

    DBObjectList= t.Union[
        t.List[obj.Table],
        t.List[obj.View],
        t.List[obj.Index],
        t.List[obj.Sequence],
        t.List[obj.Enum],
        t.List[obj.Function],
        t.List[obj.Trigger],
        t.List[obj.Dependency],
    ]


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
FUNCTION_QUERY = os.path.join(SQL_DIR, "functions.sql")
TRIGGER_QUERY = os.path.join(SQL_DIR, "triggers.sql")
DEPENDENCY_QUERY = os.path.join(SQL_DIR, "dependencies.sql")

queries: "t.Dict[DBObjectType, str]" = {
    "table": TABLE_QUERY,
    "view": VIEW_QUERY,
    "index": INDEX_QUERY,
    "sequence": SEQUENCE_QUERY,
    "enum": ENUM_QUERY,
    "function": FUNCTION_QUERY,
    "trigger": TRIGGER_QUERY,
    "dependency": DEPENDENCY_QUERY,
}


@te.overload
def query(cursor, obj_type: te.Literal["table"]) -> t.List[obj.Table]: ...
@te.overload
def query(cursor, obj_type: te.Literal["view"]) -> t.List[obj.View]: ...
@te.overload
def query(cursor, obj_type: te.Literal["index"]) -> t.List[obj.Index]: ...
@te.overload
def query(cursor, obj_type: te.Literal["sequence"]) -> t.List[obj.Sequence]: ...
@te.overload
def query(cursor, obj_type: te.Literal["enum"]) -> t.List[obj.Enum]: ...
@te.overload
def query(cursor, obj_type: te.Literal["function"]) -> t.List[obj.Function]: ...
@te.overload
def query(cursor, obj_type: te.Literal["trigger"]) -> t.List[obj.Trigger]: ...
@te.overload
def query(cursor, obj_type: te.Literal["dependency"]) -> t.List[obj.Dependency]: ...
def query(cursor, obj_type: "DBObjectType") -> "DBObjectList":
    q = queries[obj_type]
    with open(q, "r") as f:
        sql = f.read()
    cursor.execute(sql)
    results = []
    for record in cursor:
        result = dict(**{"obj_type": obj_type, **record})
        results.append(result)
    return results  # type: ignore


def make_sequence_create(sequence: obj.Sequence) -> str:
    rv = "CREATE SEQUENCE %s" % sequence["name"]
    rv += " AS %s" % sequence["data_type"]
    rv += " INCREMENT BY %s" % sequence["increment"]

    if sequence["minimum_value"]:
        rv += " MINVALUE %s" % sequence["minimum_value"]
    else:
        rv += " NO MINVALUE"

    if sequence["minimum_value"]:
        rv += " MAXVALUE %s" % sequence["maximum_value"]
    else:
        rv += " NO MAXVALUE"

    if sequence["start_value"]:
        rv += " START WITH %s" % sequence["start_value"]

    if sequence["cycle_option"]:
        rv += " CYCLE"
    else:
        rv += " NO CYCLE"

    return rv


def make_enum_create(enum: obj.Enum) -> str:
    return "CREATE TYPE %s AS ENUM (%s)" % (
        get_obj_id(enum),
        ", ".join("'%s'" % e for e in enum["elements"])
    )


def make_constraint(constraint: obj.Constraint) -> str:
    return "CONSTRAINT %s %s" % constraint


def make_table_create(table: obj.Table) -> str:
    column_statements = []
    for col_name in table["columns"]:
        column = get_column(table, col_name)
        column_str = make_column(column)
        column_statements.append(column_str);
    rv = "CREATE {}TABLE {} ({}".format(
        "UNLOGGED" if table["persistence"] == "u" else "",
        table["name"],
        ", ".join(column_statements)
    )
    constraints = [
        make_constraint(get_constraint(table, c))
        for c in table["constraints"]
    ]
    if constraints:
        rv = "{}, {})".format(rv, ", ".join(constraints))
    else:
        rv = rv + ")"
    return rv

def make_column(column: obj.Column) -> str:
    name, type, default, is_notnull = column
    notnull = " NOT NULL" if is_notnull else ""
    default_key = " DEFAULT" if default != "NULL" else ""
    default_val = " %s" % default if default != "NULL" else ""
    return "{name} {type}{notnull}{default_key}{default_val}".format(
        name=name,
        type=type,
        notnull=notnull,
        default_key=default_key,
        default_val=default_val,
    )


def get_obj_id(obj: obj.DBObject) -> str:
    if obj["obj_type"] == "table":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "view":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "index":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "sequence":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "enum":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "function":
        return "%s.%s" % (obj["schema"], obj["signature"])
    if obj["obj_type"] == "trigger":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "dependency":
        return "%s.%s" % (obj["identity"], obj["dependency_identity"])
    raise ValueError("Invalid obj: %s" % obj)


def get_column(table: obj.Table, name: str) -> obj.Column:
    i = table["columns"].index(name)
    return (
        table["columns"][i],
        table["column_types"][i],
        table["column_defaults"][i],
        table["not_null_columns"][i],
    )


def get_constraint(table: obj.Table, name: str) -> obj.Constraint:
    i = table["constraints"].index(name)
    return (
        table["constraints"][i],
        table["constraint_definitions"][i],
    )


def format_statement(statement: str) -> str:
    statement = statement.strip()
    if not statement.endswith(";"):
        statement = statement + ";"
    return statement


def topo_sort(graph: "nx.DiGraph", items: t.Dict[str, t.List[str]]) -> t.List[str]:
    rv = []
    for dep in nx.topological_sort(graph):
        try:
            rv.extend(items.pop(dep))
        except KeyError:
            pass

    for _, statements in items.items():
        rv.extend(statements)

    return rv
