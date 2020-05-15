from collections import defaultdict
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
def diff_table(source: obj.Table, target: obj.Table) -> t.List[str]:
    alterations = []
    alterations.extend(diff_columns(source, target))
    alterations.extend(diff_constraints(source, target))
    if alterations:
        table_id = helpers.get_obj_id(target)
        statement = "ALTER TABLE {name} {alterations}".format(
            name=table_id,
            alterations=", ".join(alterations),
        )
        return [statement]
    return []


@register_diff("view")
def diff_view(source: obj.View, target: obj.View) -> t.List[str]:
    if source["definition"] != target["definition"]:
        statement = (
            "CREATE OR REPLACE VIEW %s AS\n" % helpers.get_obj_id(target)
        ) + target["definition"]
        return [statement]
    return []


@register_diff("index")
def diff_index(source: obj.Index, target: obj.Index) -> t.List[str]:
    return []


@register_diff("function")
def diff_function(source: obj.Function, target: obj.Function) -> t.List[str]:
    if source["definition"] != target["definition"]:
        # TODO definition needs to be CREATE OR REPLACE
        return [target["definition"]]
    return []


@register_diff("trigger")
def diff_trigger(source: obj.Trigger, target: obj.Trigger) -> t.List[str]:
    if source["definition"] != target["definition"]:
        drop = "DROP TRIGGER %s" % helpers.get_obj_id(source)
        create = target["definition"]
        return [drop, create]
    return []


@register_diff("enum")
def diff_enum(source: obj.Enum, target: obj.Enum) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["elements"]), set(target["elements"]))
    if source_unique:
        enum_id = helpers.get_obj_id(source)
        drop = "DROP TYPE %s" % enum_id
        create = helpers.make_enum_create(target)
        rv.extend([drop, create])
        return rv

    for el in target_unique:
        enum_id = helpers.get_obj_id(target)
        alter = "ALTER TYPE %s ADD VALUE '%s'" % (enum_id, el)
        rv.append(alter)

    return rv


@register_drop("trigger")
def drop_trigger(trigger: obj.Trigger) -> str:
    return "DROP TRIGGER %s" % helpers.get_obj_id(trigger)


@register_create("trigger")
def create_trigger(trigger: obj.Trigger) -> str:
    return trigger["definition"]


@register_drop("function")
def drop_function(function: obj.Function) -> str:
    return "DROP FUNCTION %s" % helpers.get_obj_id(function)


@register_create("function")
def create_function(function: obj.Trigger) -> str:
    return function["definition"]


@register_drop("enum")
def drop_enum(enum: obj.Enum) -> str:
    return "DROP TYPE %s" % helpers.get_obj_id(enum)


@register_create("enum")
def create_enum(enum: obj.Enum) -> str:
    return helpers.make_enum_create(enum)


@register_drop("sequence")
def drop_sequence(sequence: obj.Sequence) -> str:
    return "DROP SEQUENCE %s" % helpers.get_obj_id(sequence)


@register_create("sequence")
def create_sequence(sequence: obj.Sequence) -> str:
    return helpers.make_sequence_create(sequence)


@register_drop("index")
def drop_index(index: obj.Index) -> str:
    return "DROP INDEX %s" % helpers.get_obj_id(index)


@register_create("index")
def create_index(index: obj.Index) -> str:
    if not index["is_unique"] and not index["is_pk"]:
        return index["definition"]
    return ""


@register_drop("view")
def drop_view(view: obj.View) -> str:
    return "DROP VIEW %s" % helpers.get_obj_id(view)


@register_create("view")
def create_view(view: obj.View) -> str:
    return (
        "CREATE VIEW %s AS\n" % helpers.get_obj_id(view)
    ) + view["definition"]


@register_drop("table")
def drop_table(table: obj.Table) -> str:
    return "DROP TABLE %s" % helpers.get_obj_id(table)


@register_create("table")
def create_table(table: obj.Table) -> str:
    return helpers.make_table_create(table)


def diff(source: obj.DBObject, target: obj.DBObject) -> t.List[str]:
    try:
        handler = diff_handlers[source["obj_type"]]
    except KeyError:
        return []
    return handler(source, target)


def create(obj: obj.DBObject) -> str:
    handler = create_handlers[obj["obj_type"]]
    return handler(obj)


def drop(obj: obj.DBObject) -> str:
    handler = drop_handlers[obj["obj_type"]]
    return handler(obj)
