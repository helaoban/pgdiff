import typing as t


if t.TYPE_CHECKING:
    from .objects import Table, DBObject, Index, View, Sequence, Enum
    Column = t.Tuple[str, str, str, bool]
    Constraint = t.Tuple[str, str]


def make_sequence_create(sequence: "Sequence") -> str:
    rv = "CREATE SEQUENCE %s" % sequence["name"]
    rv += " AS %s" % sequence["data_type"]
    rv += " INCREMENT BY %s" % sequence["increment"]

    if sequence["minimum_value"]:
        rv += " MINVALUE %s" % sequence["minimum_value"]
    else:
        rv += " NO MINVALUE"

    if sequence["minimum_value"]:
        rv += " MAXVALUE %s" % sequence["maximum_value"]
    else:
        rv += " NO MAXVALUE"

    if sequence["start_value"]:
        rv += " START WITH %s" % sequence["start_value"]

    if sequence["cycle_option"]:
        rv += " CYCLE"
    else:
        rv += " NO CYCLE"

    return rv


def make_enum_create(enum: "Enum") -> str:
    return "CREATE TYPE %s AS ENUM (%s)" % (
        enum["name"],
        ", ".join("'%s'" % e for e in enum["elements"])
    )


def make_table_create(table: "Table") -> str:
    column_statements = []
    for col_name in table["columns"]:
        column = get_column(table, col_name)
        column_str = make_column_add(column)
        column_statements.append(column_str);
    return "CREATE {}TABLE {} ({})".format(
        "UNLOGGED" if table["persistence"] == "u" else "",
        table["name"],
        ", ".join(column_statements)
    )


def make_column_add(column: "Column") -> str:
    name, type, default, notnull = column
    default_key = " DEFAULT" if default != "NULL" else ""
    default_val = " %s" % default if default != "NULL" else ""
    return "ADD COLUMN {name} {type}{default_key}{default_val}".format(
        name=name,
        type=type,
        default_key=default_key,
        default_val=default_val,
    )


def get_obj_id(obj: "DBObject") -> str:
    if obj["obj_type"] == "table":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "view":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "index":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "sequence":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "enum":
        return "%s.%s" % (obj["schema"], obj["name"])
    if obj["obj_type"] == "function":
        return "%s.%s" % (obj["schema"], obj["signature"])
    raise ValueError("Invalid obj: %s" % obj)


def get_column(table: "Table", name: str) -> "Column":
    i = table["columns"].index(name)
    return (
        table["columns"][i],
        table["column_types"][i],
        table["column_defaults"][i],
        table["not_null_columns"][i],
    )


def get_constraint(table: "Table", name: str) -> "Constraint":
    i = table["constraints"].index(name)
    return (
        table["constraints"][i],
        table["constraint_definitions"][i],
    )
