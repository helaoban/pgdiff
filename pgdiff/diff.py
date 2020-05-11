import os
import sys
import typing as t
from . import objects as obj, helpers


def diff_identifiers(
    source: t.Set[str],
    target: t.Set[str],
) -> obj.DatabaseIdDiff:
    common = source & target
    unique_to_source = source - target
    unique_to_target = target - source
    return common, unique_to_source, unique_to_target


def diff_index(source: obj.Index, target: obj.Index) -> t.List[str]:
    return []


def diff_view(source: obj.View, target: obj.View) -> t.Optional[str]:
    if source["definition"] != target["definition"]:
        return (
            "CREATE OR REPLACE VIEW %s\n" % helpers.get_obj_id(target)
        ) + target["definition"]
    return None


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
        rv.append(helpers.make_column_add(col))
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
        _, source_definition = helpers.get_constraint(source, constraint_name)
        _, target_definition = helpers.get_constraint(target, constraint_name)
        if source_definition != target_definition:
            drop = "DROP CONSTRAINT %s" % constraint_name
            add = "ADD %s %s" % (constraint_name, target_definition)
            rv.extend([drop, add])
    return rv


def diff_table(source: obj.Table, target: obj.Table) -> t.Optional[str]:
    alterations = []
    alterations.extend(diff_columns(source, target))
    alterations.extend(diff_constraints(source, target))
    if alterations:
        table_id = helpers.get_obj_id(target)
        return "ALTER TABLE {name} {alterations}".format(
            name=table_id,
            alterations=" ".join(alterations),
        )
    return None


def diff_triggers(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["triggers"]), set(target["triggers"]))
    for trigger_id in source_unique:
        drop = "DROP TRIGGER %s" % trigger_id
        rv.append(drop)
    for trigger_id in target_unique:
        target_trigger = target["triggers"][trigger_id]
        rv.append(target_trigger["definition"])
    for trigger_id in common:
        source_trigger = source["triggers"][trigger_id]
        target_trigger = target["triggers"][trigger_id]
        if source_trigger["definition"] != target_trigger["definition"]:
            drop = "DROP TRIGGER %s" % trigger_id
            rv.extend([drop, target_trigger["definition"]])
    return rv


def diff_function(source: obj.Function, target: obj.Function) -> t.Optional[str]:
    if source["definition"] != target["definition"]:
        # TODO definition needs to be CREATE OR REPLACE
        return target["definition"]
    return None


def diff_functions(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["functions"]), set(target["functions"]))
    for function_id in source_unique:
        source_function = source["functions"][function_id]
        drop = "DROP FUNCTION %s" % function_id
        rv.append(drop)
    for function_id in target_unique:
        target_function = target["functions"][function_id]
        rv.append(target_function["definition"])
    for function_id in common:
        source_function = source["functions"][function_id]
        target_function = target["functions"][function_id]
        diff = diff_function(source_function, target_function)
        if diff:
            rv.append(diff)
    return rv


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


def diff_enums(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    common, source_unique, target_unique = diff_identifiers(
        set(source["enums"]), set(target["enums"]))
    for enum_id in source_unique:
        rv.append("DROP TYPE %s" % enum_id)
    for enum_id in target_unique:
        target_enum = target["enums"][enum_id]
        rv.append(helpers.make_enum_create(target_enum))
    for enum_id in common:
        source_enum = source["enums"][enum_id]
        target_enum = target["enums"][enum_id]
        rv.extend(diff_enum(source_enum, target_enum))
    return rv


def diff_sequences(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv =  []
    common, source_unique, target_unique = diff_identifiers(
        set(source["sequences"]), set(target["sequences"]))

    for sequence_id in source_unique:
        rv.append("DROP SEQUENCE %s" % sequence_id)

    for sequence_id in target_unique:
        target_sequence = target["sequences"][sequence_id]
        rv.append(helpers.make_sequence_create(target_sequence))

    return rv


def diff_indices(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []

    common, source_unique, target_unique = diff_identifiers(
        set(source["indices"]), set(target["indices"]))

    for index_id in source_unique:
        rv.append("DROP INDEX %s" % index_id)

    for index_id in target_unique:
        target_index = target["indices"][index_id]
        rv.append(target_index["definition"])

    for index_id in common:
        source_index = source["indices"][index_id]
        target_index = target["indices"][index_id]
        index_diff = diff_index(source_index, target_index)
        if index_diff:
            rv.extend(index_diff)

    return rv


def diff_views(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []

    common, source_unique, target_unique = diff_identifiers(
        set(source["views"]), set(target["views"]))

    for view_id in source_unique:
        rv.append("DROP VIEW %s" % view_id)

    for view_id in target_unique:
        target_view = target["views"][view_id]
        statement = (
            "CREATE VIEW %s\n" % view_id
        ) + target_view["definition"]
        rv.append(statement)

    for view_id in common:
        source_view = source["views"][view_id]
        target_view = target["views"][view_id]
        view_diff = diff_view(source_view, target_view)
        if view_diff:
            rv.append(view_diff)

    return rv


def diff_tables(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []

    common, source_unique, target_unique = diff_identifiers(
        set(source["tables"]), set(target["tables"]))

    for table_id in source_unique:
        rv.append("DROP TABLE %s" % table_id)

    for table_id in target_unique:
        target_table = target["tables"][table_id]
        rv.append(helpers.make_table_create(target_table))

    for table_id in common:
        source_table = source["tables"][table_id]
        target_table = target["tables"][table_id]
        table_diff = diff_table(source_table, target_table)
        if table_diff:
            rv.append(table_diff)

    return rv


def diff(source: obj.Database, target: obj.Database) -> t.List[str]:
    rv = []
    rv.extend(diff_tables(source, target))
    rv.extend(diff_views(source, target))
    rv.extend(diff_indices(source, target))
    rv.extend(diff_sequences(source, target))
    rv.extend(diff_enums(source, target))
    rv.extend(diff_functions(source, target))
    rv.extend(diff_triggers(source, target))
    return rv
