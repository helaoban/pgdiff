from itertools import chain
import typing as t
from . import objects, helpers


diff_handlers = {}
create_handlers = {}
drop_handlers = {}


if t.TYPE_CHECKING:
    WR = t.TypeVar("WR", bound=t.Callable)
    Wrapper = t.Callable[[WR], WR]
    DiffHandler = t.Callable[
        [dict, t.Any, t.Any],
        t.Iterable[str]
    ]
    CreateHandler = t.Callable[
        [dict, t.Any],
        t.Iterable[str]
    ]
    DropHandler = t.Callable[
        [dict, t.Any],
        t.Iterable[str]
    ]


def register_diff(type_: str) -> "Wrapper[DiffHandler]":
    def wrapped(func: "DiffHandler"):
        diff_handlers[type_] = func
        return func
    return wrapped


def register_create(type_: str) -> "Wrapper[CreateHandler]":
    def wrapped(func: "CreateHandler"):
        create_handlers[type_] = func
        return func
    return wrapped


def register_drop(type_: str) -> "Wrapper[DropHandler]":
    def wrapped(func: "DropHandler"):
        drop_handlers[type_] = func
        return func
    return wrapped


def diff_identifiers(
    source: t.Set[str],
    target: t.Set[str],
) -> objects.DatabaseIdDiff:
    common = source & target
    unique_to_source = source - target
    unique_to_target = target - source
    return common, unique_to_source, unique_to_target


def diff_column(source: objects.Column, target: objects.Column) -> t.Iterator[str]:
    if source["type"] != target["type"]:
        yield "ALTER COLUMN %s TYPE %s" % (target["name"], target["type"])

    if source["default"] != target["default"]:
        if target["default"] is None:
            yield "ALTER COLUMN %s DROP DEFAULT" % target["name"]
        else:
            yield "ALTER COLUMN %s SET DEFAULT %s" % (
                target["name"], target["default"])

    if source["not_null"] != target["not_null"]:
        if target["not_null"] is True:
            yield "ALTER COLUMN %s SET NOT NULL" % target["name"]
        else:
            yield "ALTER COLUMN %s DROP NOT NULL" % target["name"]


def diff_columns(source: objects.Table, target: objects.Table) -> t.Iterator[str]:
    source_columns = {c["name"]: c for c in source["columns"]}
    target_columns = {c["name"]: c for c in target["columns"]}
    common, source_unique, target_unique = diff_identifiers(
        set(source_columns.keys()), set(target_columns))
    for name in common:
        source_col = source_columns[name]
        target_col = target_columns[name]
        yield from diff_column(source_col, target_col)
    for name in source_unique:
        yield "DROP COLUMN %s" % name
    for name in target_unique:
        col = target_columns[name]
        yield "ADD COLUMN %s" % helpers.make_column(col)


def diff_constraint(
    source: objects.Constraint,
    target: objects.Constraint
) -> t.Iterator[str]:
    if source["definition"] != target["definition"]:
        yield "DROP CONSTRAINT %s" % source["name"]
        yield "ADD CONSTRAINT %s %s" % (source["name"], target["definition"])


@register_diff("constraint")
def diff_constraints(
    ctx: dict,
    source: objects.Table,
    target: objects.Table
) -> t.Iterator[str]:
    source_constraints = {c["name"]: c for c in source["constraints"]}
    target_constraints = {c["name"]: c for c in target["constraints"]}
    common, source_unique, target_unique = diff_identifiers(
        set(source_constraints.keys()), set(target_constraints))
    for name in source_unique:
        yield "DROP CONSTRAINT %s" % name
    for name in target_unique:
        constraint = target_constraints[name]
        yield "ADD CONSTRAINT %s %s" % (name, constraint["definition"])
    for name in common:
        source_constraint = source_constraints[name]
        target_constraint = target_constraints[name]
        yield from diff_constraint(source_constraint, target_constraint)


@register_diff("table")
def diff_table(
    ctx: dict,
    source: objects.Table,
    target: objects.Table
) -> t.Iterator[str]:
    alterations = list(chain(
        diff_constraints(ctx, source, target),
        diff_columns(source, target),
    ))
    if alterations:
        yield "ALTER TABLE {name} {alterations}".format(
            name=target["identity"],
            alterations=", ".join(alterations),
        )


@register_diff("view")
def diff_view(
    ctx: dict,
    source: objects.View,
    target: objects.View
) -> t.Iterator[str]:
    if source["definition"] != target["definition"]:
        if source["identity"] not in ctx["dropped"]:
            yield from drop_view(ctx, target)
        yield from create_view(ctx, target)


@register_diff("index")
def diff_index(
    ctx: dict,
    source: objects.Index,
    target: objects.Index
) -> t.List[str]:
    return []


@register_diff("sequence")
def diff_sequence(
    ctx: dict,
    source: objects.Sequence,
    target: objects.Sequence
) -> t.List[str]:
    return []


@register_diff("function")
def diff_function(
    ctx: dict,
    source: objects.Function,
    target: objects.Function
) -> t.Iterator[str]:
    if source["definition"] != target["definition"]:
        # TODO definition needs to be CREATE OR REPLACE
        yield target["definition"]


@register_diff("trigger")
def diff_trigger(
    ctx: dict,
    source: objects.Trigger,
    target: objects.Trigger
) -> t.Iterator[str]:
    if source["definition"] != target["definition"]:
        yield from drop(ctx, source)
        yield from create(ctx, target)


@register_diff("enum")
def diff_enum(ctx: dict, source: objects.Enum, target: objects.Enum) -> t.Iterator[str]:
    _, source_unique, target_unique = diff_identifiers(
        set(source["elements"]), set(target["elements"]))
    if source_unique:
        yield from drop(ctx, source)
        yield from create(ctx, target)
    for ele in target_unique:
        yield "ALTER TYPE %s ADD VALUE '%s'" % (target["identity"], ele)


@register_drop("trigger")
def drop_trigger(ctx: dict, trigger: objects.Trigger) -> t.Iterator[str]:
    yield "DROP TRIGGER %s ON %s" % (trigger["name"], trigger["table_name"])


@register_create("trigger")
def create_trigger(ctx: dict, trigger: objects.Trigger) -> t.Iterator[str]:
    yield trigger["definition"]


@register_drop("function")
def drop_function(ctx: dict, function: objects.Function) -> t.Iterator[str]:
    yield "DROP FUNCTION %s" % function["identity"]


@register_create("function")
def create_function(ctx: dict, function: objects.Trigger) -> t.Iterator[str]:
    yield function["definition"]


@register_drop("enum")
def drop_enum(ctx: dict, enum: objects.Enum) -> t.Iterator[str]:
    yield "DROP TYPE %s" % enum["identity"]


@register_create("enum")
def create_enum(ctx: dict, enum: objects.Enum) -> t.Iterator[str]:
    yield helpers.make_enum_create(enum)


@register_drop("sequence")
def drop_sequence(ctx: dict, sequence: objects.Sequence) -> t.Iterator[str]:
    yield "DROP SEQUENCE %s" % sequence["identity"]


@register_create("sequence")
def create_sequence(ctx: dict, sequence: objects.Sequence) -> t.Iterator[str]:
    yield helpers.make_sequence_create(sequence)


@register_drop("index")
def drop_index(ctx: dict, index: objects.Index) -> t.Iterator[str]:
    if not index["from_constraint"]:
        yield "DROP INDEX %s" % index["identity"]


@register_create("index")
def create_index(ctx: dict, index: objects.Index) -> t.Iterator[str]:
    if not index["from_constraint"]:
        yield index["definition"]


@register_drop("view")
def drop_view(ctx: dict, view: objects.View) -> t.Iterator[str]:
    yield "DROP VIEW %s" % view["identity"]


@register_create("view")
def create_view(ctx: dict, view: objects.View) -> t.Iterator[str]:
    yield (
        "CREATE VIEW %s AS\n" % view["identity"]
    ) + view["definition"]


@register_drop("table")
def drop_table(ctx: dict, table: objects.Table) -> t.Iterator[str]:
    yield "DROP TABLE %s" % table["identity"]


@register_create("table")
def create_table(ctx: dict, table: objects.Table) -> t.Iterator[str]:
    yield helpers.make_table_create(table)


def diff(
    ctx: dict,
    source: objects.DBObject,
    target: objects.DBObject
) -> t.Iterable[str]:
    handler = diff_handlers[source["obj_type"]]
    return handler(ctx, source, target)


def create(
    ctx: dict,
    obj: objects.DBObject
) -> t.Iterable[str]:
    handler = create_handlers[obj["obj_type"]]
    return handler(ctx, obj)


def drop(
    ctx: dict,
    obj: objects.DBObject
) -> t.Iterable[str]:
    handler = drop_handlers[obj["obj_type"]]
    return handler(ctx, obj)
