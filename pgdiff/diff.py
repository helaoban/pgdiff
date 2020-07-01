from collections import defaultdict
from contextlib import contextmanager
import os
import sys
import typing as t
import networkx as nx  # type: ignore
from . import objects as obj, helpers


diff_handlers = {}
create_handlers = {}
drop_handlers = {}


def register_diff(type: str):
    def wrapped(func):
        diff_handlers[type] = func
        return func
    return wrapped


def register_create(type: str):
    def wrapped(func):
        create_handlers[type] = func
        return func
    return wrapped


def register_drop(type: str):
    def wrapped(func):
        drop_handlers[type] = func
        return func
    return wrapped


@contextmanager
def dependent_views(
    ctx: dict,
    obj: obj.DBObject,
    statements: t.List[str],
) -> t.Iterator[None]:
    descendants = []
    for obj in ctx["target_inspect"].descendants(obj["identity"]):
        if obj["obj_type"] == "view":
            descendants.append(obj)
    for d in reversed(descendants):
        statements.append(drop_view(ctx, d))
    yield
    for d in descendants:
        statements.append(create_view(ctx, d))


def diff_identifiers(
    source: t.Set[str],
    target: t.Set[str],
) -> obj.DatabaseIdDiff:
    common = source & target
    unique_to_source = source - target
    unique_to_target = target - source
    return common, unique_to_source, unique_to_target


def diff_column(source: obj.Column, target: obj.Column) -> t.List[str]:
    rv = []
    sname, stype, sdefault, snotnull = source
    tname, ttype, tdefault, tnotnull = target

    if stype != ttype:
        change = "ALTER COLUMN %s TYPE %s" % (tname, ttype)
        rv.append(change)

    if sdefault != tdefault:
        if tdefault is None:
            change = "ALTER COLUMN %s DROP DEFAULT" % tname
        else:
            change = "ALTER COLUMN %s SET DEFAULT %s" % (tname, tdefault)
        rv.append(change)

    if snotnull != tnotnull:
        if tnotnull is True:
            change = "ALTER COLUMN %s SET NOT NULL" % tname
        else:
            change = "ALTER COLUMN %s DROP NOT NULL" % tname
        rv.append(change)

    return rv


def diff_columns(source: obj.Table, target: obj.Table) -> t.List[str]:
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
        rv.append("ADD COLUMN %s" % helpers.make_column(col))
    return rv


def diff_constraint(source, target) -> t.List[str]:
    rv = []
    constraint_name, source_definition = source
    _, target_definition = target
    if source_definition != target_definition:
        drop = "DROP CONSTRAINT %s" % constraint_name
        add = "ADD %s %s" % (constraint_name, target_definition)
        rv.extend([drop, add])
    return rv


def diff_constraints(source: obj.Table, target: obj.Table) -> t.List[str]:
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
        source_constraint = helpers.get_constraint(source, constraint_name)
        target_constraint = helpers.get_constraint(target, constraint_name)
        rv.extend(diff_constraint(source_constraint, target_constraint))
    return rv


@register_diff("table")
def diff_table(ctx: dict, source: obj.Table, target: obj.Table) -> t.List[str]:
    statements: t.List[str] = []
    alterations = []
    alterations.extend(diff_columns(source, target))
    alterations.extend(diff_constraints(source, target))
    if alterations:
        with dependent_views(ctx, target, statements):
            table_id = target["identity"]
            statement = "ALTER TABLE {name} {alterations}".format(
                name=table_id,
                alterations=", ".join(alterations),
            )
            statements.append(statement)
    return statements


@register_diff("view")
def diff_view(
    ctx: dict,
    source: obj.View,
    target: obj.View
) -> t.List[str]:
    statements: t.List[str] = []
    if source["definition"] == target["definition"]:
        return statements
    with dependent_views(ctx, target, statements):
        statements.append(drop_view(ctx, target))
        statements.append(create_view(ctx, target))
    return statements


@register_diff("index")
def diff_index(
    ctx: dict,
    source: obj.Index,
    target: obj.Index
) -> t.List[str]:
    return []


@register_diff("function")
def diff_function(
    ctx: dict,
    source: obj.Function,
    target: obj.Function
) -> t.List[str]:
    if source["definition"] != target["definition"]:
        # TODO definition needs to be CREATE OR REPLACE
        return [target["definition"]]
    return []


@register_diff("trigger")
def diff_trigger(
    ctx: dict,
    source: obj.Trigger,
    target: obj.Trigger
) -> t.List[str]:
    if source["definition"] != target["definition"]:
        drop = "DROP TRIGGER %s" % source["identity"]
        create = target["definition"]
        return [drop, create]
    return []


@register_diff("enum")
def diff_enum(ctx: dict, source: obj.Enum, target: obj.Enum) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["elements"]), set(target["elements"]))
    if source_unique:
        enum_id = source["identity"]
        drop = "DROP TYPE %s" % enum_id
        create = helpers.make_enum_create(target)
        rv.extend([drop, create])
        return rv

    for el in target_unique:
        enum_id = target["identity"]
        alter = "ALTER TYPE %s ADD VALUE '%s'" % (enum_id, el)
        rv.append(alter)

    return rv


@register_drop("trigger")
def drop_trigger(ctx: dict, trigger: obj.Trigger) -> str:
    return "DROP TRIGGER %s" % trigger["identity"]


@register_create("trigger")
def create_trigger(ctx: dict, trigger: obj.Trigger) -> str:
    return trigger["definition"]


@register_drop("function")
def drop_function(ctx: dict, function: obj.Function) -> str:
    return "DROP FUNCTION %s" % function["identity"]


@register_create("function")
def create_function(ctx: dict, function: obj.Trigger) -> str:
    return function["definition"]


@register_drop("enum")
def drop_enum(ctx: dict, enum: obj.Enum) -> str:
    return "DROP TYPE %s" % enum["identity"]


@register_create("enum")
def create_enum(ctx: dict, enum: obj.Enum) -> str:
    return helpers.make_enum_create(enum)


@register_drop("sequence")
def drop_sequence(ctx: dict, sequence: obj.Sequence) -> str:
    return "DROP SEQUENCE %s" % sequence["identity"]


@register_create("sequence")
def create_sequence(ctx: dict, sequence: obj.Sequence) -> str:
    return helpers.make_sequence_create(sequence)


@register_drop("index")
def drop_index(ctx: dict, index: obj.Index) -> str:
    return "DROP INDEX %s" % index["identity"]


@register_create("index")
def create_index(ctx: dict, index: obj.Index) -> str:
    if not index["is_unique"] and not index["is_pk"]:
        return index["definition"]
    return ""


@register_drop("view")
def drop_view(ctx: dict, view: obj.View) -> str:
    return "DROP VIEW %s" % view["identity"]


@register_create("view")
def create_view(ctx: dict, view: obj.View) -> str:
    return (
        "CREATE VIEW %s AS\n" % view["identity"]
    ) + view["definition"]


@register_drop("table")
def drop_table(ctx: dict, table: obj.Table) -> str:
    return "DROP TABLE %s" % table["identity"]


@register_create("table")
def create_table(ctx: dict, table: obj.Table) -> str:
    return helpers.make_table_create(table)


def diff(
    ctx: dict,
    source: obj.DBObject,
    target: obj.DBObject
) -> t.List[str]:
    try:
        handler = diff_handlers[source["obj_type"]]
    except KeyError:
        return []
    return handler(ctx, source, target)


def create(
    ctx: dict,
    obj: obj.DBObject
) -> str:
    handler = create_handlers[obj["obj_type"]]
    return handler(ctx, obj)


def drop(
    ctx: dict,
    obj: obj.DBObject
) -> str:
    handler = drop_handlers[obj["obj_type"]]
    return handler(ctx, obj)
