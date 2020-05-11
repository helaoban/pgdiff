import os
import typing as t
from . import objects as obj, helpers

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
