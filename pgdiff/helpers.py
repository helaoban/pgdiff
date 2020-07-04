import os
import typing as t
import typing_extensions as te

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
    ]

    ValidQueryType = t.Union[DBObjectType, te.Literal["dependency"]]

    DBObjectList = t.Union[
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
}


@te.overload
def query(cursor, obj_type: te.Literal["table"]) -> t.Iterator[obj.Table]: ...
@te.overload
def query(cursor, obj_type: te.Literal["view"]) -> t.Iterator[obj.View]: ...
@te.overload
def query(cursor, obj_type: te.Literal["index"]) -> t.Iterator[obj.Index]: ...
@te.overload
def query(cursor, obj_type: te.Literal["sequence"]) -> t.Iterator[obj.Sequence]: ...
@te.overload
def query(cursor, obj_type: te.Literal["enum"]) -> t.Iterator[obj.Enum]: ...
@te.overload
def query(cursor, obj_type: te.Literal["function"]) -> t.Iterator[obj.Function]: ...
@te.overload
def query(cursor, obj_type: te.Literal["trigger"]) -> t.Iterator[obj.Trigger]: ...
@te.overload
def query(cursor, obj_type: te.Literal["dependency"]) -> t.Iterator[obj.Dependency]: ...
def query(
    cursor,
    obj_type: "ValidQueryType",
) -> t.Iterator[t.Union[obj.DBObject, obj.Dependency]]:
    q = DEPENDENCY_QUERY if obj_type == "dependency" else queries[obj_type]
    with open(q, "r") as file:
        sql = file.read()
    cursor.execute(sql)
    for record in cursor:
        yield dict(**{"obj_type": obj_type, **record})  # type: ignore


def query_objects(cursor) -> t.Iterator[obj.DBObject]:
    for k in queries:
        for o in query(cursor, k):
            yield o


def query_dependencies(cursor) -> t.Iterator[obj.Dependency]:
    return query(cursor, "dependency")


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
        enum["identity"],
        ", ".join("'%s'" % e for e in enum["elements"])
    )


def make_constraint(constraint: obj.Constraint) -> str:
    return "CONSTRAINT %s %s" % (
        constraint["name"], constraint["definition"])


def make_table_create(table: obj.Table) -> str:
    column_statements = []
    for col in table["columns"]:
        column_statements.append(make_column(col))
    rv = "CREATE {}TABLE {} ({}".format(
        "UNLOGGED" if table["persistence"] == "u" else "",
        table["name"],
        ", ".join(column_statements)
    )
    constraints = [
        make_constraint(c)
        for c in table["constraints"]
    ]
    if constraints:
        rv = "{}, {})".format(rv, ", ".join(constraints))
    else:
        rv = rv + ")"
    return rv


def make_column(column: obj.Column) -> str:
    default = column["default"]
    notnull = " NOT NULL" if column["not_null"] else ""
    default_key = " DEFAULT" if default != "NULL" else ""
    default_val = " %s" % default if default != "NULL" else ""
    return "{name} {type}{notnull}{default_key}{default_val}".format(
        name=column["name"],
        type=column["type"],
        notnull=notnull,
        default_key=default_key,
        default_val=default_val,
    )


def format_statement(statement: str) -> str:
    statement = statement.strip()
    if not statement.endswith(";"):
        statement = statement + ";"
    return statement
