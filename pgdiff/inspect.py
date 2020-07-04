from collections import defaultdict, OrderedDict
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

            # TODO should there every be a situation where
            # we have a dependency but not the object?
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
        return reversed(list(self))

    def ancestors(self, obj_id: str) -> t.Iterator[obj.DBObject]:
        sg = self.graph.subgraph(nx.ancestors(self.graph, obj_id))
        for obj_id in reversed(list(nx.topological_sort(sg))):
            yield self[obj_id]

    def descendants(self, obj_id: str) -> t.Iterator[obj.DBObject]:
        sg = self.graph.subgraph(nx.descendants(self.graph, obj_id))
        for obj_id in nx.topological_sort(sg):
            yield self[obj_id]

    def _diff(self, other: "Inspection") -> t.Iterator[str]:
        dropped: OrderedDict[str, None] = OrderedDict()
        ctx: dict = {"dropped": dropped}

        for target in self:
            oid = target["identity"]
            try:
                source = other[oid]
            except KeyError:
                yield from create(ctx, target)
            else:

                diffs = list(diff(ctx, source, target))
                if not diffs:
                    continue

                for d in reversed(list(other.descendants(oid))):
                    doid = d["identity"]
                    if d["obj_type"] in {"view", "function"} and doid not in dropped:
                        yield from drop(ctx, d)
                        dropped[doid] = None

                yield from diffs
                dropped.pop(oid, None)

        for doid in dropped:
            if doid in self:
                yield from create(ctx, self[doid])

        for source in reversed(other):
            soid = source["identity"]
            if soid not in self and soid not in dropped:
                yield from drop(ctx, source)

    def diff(self, other: "Inspection") -> t.List[str]:
        rv = []
        for s in self._diff(other):
            rv.append(helpers.format_statement(s))
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
