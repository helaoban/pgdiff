import os
import typing as t

import networkx as nx  # type: ignore

from . import objects as obj, helpers
from .diff import diff, create, drop


IT = t.TypeVar("IT", bound=obj.DBObject)


def _index_by_id(items: t.List[IT]) -> t.Dict[str, IT]:
    rv = {}
    for x in items:
        rv[helpers.get_obj_id(x)] = x
    return rv


def _make_graph(
    objects: t.Dict[str, obj.DBObject],
    dependencies: t.List[obj.Dependency],
) -> nx.DiGraph:
    graph = nx.DiGraph()
    for obj_id in objects:
        if "timescale" in obj_id:
            print(obj_id)
        graph.add_node(obj_id)
    for dep in dependencies:
        if dep["dependency_identity"] not in graph:
            continue
        if dep["identity"] not in graph:
            continue
        graph.add_edge(dep["dependency_identity"], dep["identity"])
    return graph


class Inspection:

    def __init__(
        self,
        objects: t.Dict[str, obj.DBObject],
        dependencies: t.List[obj.Dependency],
    ) -> None:
        self.graph = _make_graph(objects, dependencies)
        self.objects = objects

    def __getitem__(self, obj_id: str) -> obj.DBObject:
        return self.objects[obj_id]

    def __contains__(self, obj_id: str) -> bool:
        return obj_id in self.objects

    def diff(self, other: "Inspection") -> t.List[str]:
        rv = []

        for obj_id in nx.topological_sort(other.graph):
            if obj_id not in self:
                other_obj = other[obj_id]
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


def _get_objects(cursor) -> "t.List[obj.DBObject]":
    obj_types: "t.List[helpers.DBObjectType]" = [
        "table",
        "view",
        "index",
        "sequence",
        "enum",
        "function",
        "trigger",
    ]
    rv: "t.List[obj.DBObject]" = []
    for k in obj_types:
        objs = helpers.query(cursor, k)
        rv.extend(objs)
    return rv


def inspect(cursor) -> Inspection:
    objects = _get_objects(cursor)
    dependencies = helpers.query(cursor, "dependency")
    return Inspection(
        objects=_index_by_id(objects),
        dependencies=dependencies,
    )
