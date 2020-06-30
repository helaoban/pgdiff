import os
from fnmatch import fnmatch
import typing as t

import networkx as nx  # type: ignore

from . import objects as obj, helpers
from .diff import diff, create, drop


class Inspection:

    def __init__(
        self,
        objects: t.Iterable[obj.DBObject],
        dependencies: t.Iterable[obj.Dependency],
        ctx: dict,
    ) -> None:
        self.graph = nx.DiGraph()
        self.objects: t.Dict[str, obj.DBObject] = {}
        self.ctx = ctx

        self._populate_graph(objects, dependencies)

    def _populate_graph(
        self,
        objects: t.Iterable[obj.DBObject],
        dependencies: t.Iterable[obj.Dependency],
    ):
        for obj in objects:
            i = obj["identity"]
            self.graph.add_node(i)
            self.objects[i] = obj
        for dep in dependencies:
            i, di = dep["identity"], dep["dependency_identity"]
            if i in self.graph and di in self.graph:
                self.graph.add_edge(di, i)

    def __getitem__(self, obj_id: str) -> obj.DBObject:
        return self.objects[obj_id]

    def __contains__(self, obj_id: str) -> bool:
        return obj_id in self.objects

    def __iter__(self) -> t.Iterator[obj.DBObject]:
        for obj_id in nx.topological_sort(self.graph):
            yield self[obj_id]

    def __reversed__(self) -> t.Iterator[obj.DBObject]:
        for obj_id in reversed(list(self.graph)):
            yield self[obj_id]

    def diff(self, other: "Inspection") -> t.List[str]:
        rv = []
        ctx: dict = {"inspection": self, **self.ctx}

        for obj in self:
            try:
                other_obj = other[obj["identity"]]
            except KeyError:
                statement = create(ctx, obj)
                if statement:
                    rv.append(helpers.format_statement(statement))
            else:
                for s in diff(ctx, other_obj, obj):
                    rv.append(helpers.format_statement(s))

        for obj in reversed(other):
            if obj["identity"] not in self:
                other_obj = other[obj["identity"]]
                statement = drop(ctx, other_obj)
                rv.append(helpers.format_statement(statement))

        return rv


def _filter_objects(
    objects: t.Iterable[obj.DBObject],
    patterns: t.Iterable[str],
) -> t.Iterator[obj.DBObject]:
    for obj in objects:
        for pattern in patterns:
            if fnmatch(obj["schema"], pattern):
                yield obj
                break


def inspect(cursor, include: t.Optional[t.Iterable[str]] = None) -> Inspection:
    pg_version = cursor.connection.server_version
    objects = helpers.query_objects(cursor)
    if include is not None:
        objects = _filter_objects(objects, include)
    dependencies = list(helpers.query_dependencies(cursor))
    return Inspection(
        objects=objects,
        dependencies=dependencies,
        ctx={"pg_version": pg_version},
    )
