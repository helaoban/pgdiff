import os
import typing as t
import networkx as nx  # type: ignore
from . import objects as obj, helpers
from .diff import diff, create, drop


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


class Inspection:

    def __init__(
        self,
        objects: t.Dict[str, obj.DBObject],
        dependencies: t.List[obj.Dependency],
    ):

        graph = nx.DiGraph()
        for obj_id in objects:
            graph.add_node(obj_id)
        for dep in dependencies:
            graph.add_edge(dep["dependency_identity"], dep["identity"])

        self.graph = graph
        self.objects = objects

    def __getitem__(self, obj_id: str) -> obj.DBObject:
        return self.objects[obj_id]

    def __contains__(self, obj_id: str) -> bool:
        return obj_id in self.objects

    def diff(self, other: "Inspection") -> t.List[str]:
        rv = []

        for obj_id in nx.topological_sort(other.graph):
            if obj_id not in self:
                try:
                    other_obj = other[obj_id]
                except KeyError:
                    # TODO this should not be happening period.
                    continue
                statement = drop(other_obj)
                rv.append(helpers.format_statement(statement))

        for obj_id in nx.topological_sort(self.graph):
            obj = self[obj_id]
            try:
                other_obj = other[obj_id]
            except KeyError:
                statement = create(obj)
                if statement:
                    rv.append(helpers.format_statement(statement))
            else:
                for s in diff(other_obj, obj):
                    rv.append(helpers.format_statement(s))
        return rv


def inspect(cur) -> Inspection:
    tables = query(cur, TABLE_QUERY, "table")  # type: t.List[obj.Table]
    views = query(cur, VIEW_QUERY, "view")  # type: t.List[obj.View]
    indices = query(cur, INDEX_QUERY, "index")  # type: t.List[obj.Index]
    sequences = query(cur, SEQUENCE_QUERY, "sequence")  # type: t.List[obj.Sequence]
    enums = query(cur, ENUM_QUERY, "enum")  # type: t.List[obj.Enum]
    functions = query(cur, FUNCTION_QUERY, "function")  # type: t.List[obj.Function]
    triggers = query(cur, TRIGGER_QUERY, "trigger")  # type: t.List[obj.Trigger]
    dependencies = query(cur, DEPENDENCY_QUERY, "dependency")  # type: t.List[obj.Dependency]

    objects: t.List[obj.DBObject] = [
        *tables,
        *views,
        *indices,
        *sequences,
        *enums,
        *functions,
        *triggers
    ]

    return Inspection(
        objects=_index_by_id(objects),
        dependencies=dependencies,
    )
