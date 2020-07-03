from collections import defaultdict
import os
from fnmatch import fnmatch
import typing as t
from pprint import pprint

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

    def diff(self, other: "Inspection") -> t.List[str]:
        changeset: t.List[t.Tuple[str, str]] = []
        rv = []
        ctx: dict = {
            "source_inspect": other,
            "target_inspect": self,
            **self.ctx
        }

        for obj in self:
            ident = obj["identity"]
            try:
                other_obj = other[ident]
            except KeyError:
                changeset.append(("create", ident))
            else:
                drops = []
                for d in reversed(list(other.descendants(ident))):
                    if d["obj_type"] == "view":
                        drops.append(("drop", d["identity"]))

                creates = []
                for d in self.descendants(ident):
                    if d["obj_type"] == "view":
                        creates.append(("create", d["identity"]))

                changeset.extend(drops + [("alter", ident)] + creates)

        for obj in reversed(other):
            if obj["identity"] not in self:
                other_obj = other[obj["identity"]]
                changeset.append(("drop", obj["identity"]))

        occurences: t.Dict[t.Tuple[str, str], t.List[int]] = defaultdict(list)
        for i, x in enumerate(changeset):
            occurences[x].append(i)

        uniq = []
        for i, change in enumerate(changeset):
            op = change[0]
            occ = occurences[change]
            if op == "drop" and i != min(occ):
                continue
            if op == "create" and i != max(occ):
                continue
            uniq.append(change)

        for op, ident in uniq:
            if op == "alter":
                source, target = other[ident], self[ident]
                for s in diff(ctx, source, target):
                    rv.append(helpers.format_statement(s))
            if op == "drop":
                statement = drop(ctx, other[ident])
                rv.append(helpers.format_statement(statement))
            if op == "create":
                statement = create(ctx, self[ident])
                if statement:
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
