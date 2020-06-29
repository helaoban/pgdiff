import typing as t
import typing_extensions as te


if t.TYPE_CHECKING:
    import networkx as nx  # type: ignore


Column = t.Tuple[str, str, str, int]
DatabaseIdDiff = t.Tuple[t.Set[str], t.Set[str], t.Set[str]]
DBObjectFieldName = t.Union[
    te.Literal["tables"],
    te.Literal["views"],
    te.Literal["indices"],
    te.Literal["enums"],
    te.Literal["sequences"],
    te.Literal["functions"],
]
DatabaseDiff = t.Dict[DBObjectFieldName, DatabaseIdDiff]
Constraint = t.Tuple[str, str]


class Table(te.TypedDict):
    obj_type: te.Literal["table"]
    oid: str
    schema: str
    name: str
    type: str
    columns: t.List[str]
    column_types: t.List[str]
    column_defaults: t.List[t.Any]
    not_null_columns: t.List[bool]
    parent_table: str
    partition_def: str
    row_security: str
    force_row_security: str
    persistence: str
    constraints: t.List[str]
    constraint_definitions: t.List[str]


class View(te.TypedDict):
    obj_type: te.Literal["view"]
    oid: str
    schema: str
    name: str
    type: str
    definition: str


class Index(te.TypedDict):
    obj_type: te.Literal["index"]
    oid: str
    schema: str
    table_name: str
    name: str
    definition: str
    key_columns: str
    key_options: t.List[int]
    num_columns: int
    is_unique: bool
    is_pk: bool
    is_exclusion: bool
    is_immediate: bool
    is_clustered: bool
    key_expressions: str
    partial_predicate: str


class Sequence(te.TypedDict):
    obj_type: te.Literal["sequence"]
    schema: str
    name: str
    data_type: str
    precision: int
    precision_radix: int
    scale: int
    start_value: str
    minimum_value: str
    maximum_value: str
    increment: str
    cycle_option: bool


class Enum(te.TypedDict):
    obj_type: te.Literal["enum"]
    oid: str
    schema: str
    name: str
    elements: t.List[str]


class Function(te.TypedDict):
    obj_type: te.Literal["function"]
    oid: str
    schema: str
    name: str
    signature: str
    language: str
    is_strict: bool
    is_security_definer: bool
    volatility: str
    kind: str
    argnames: t.List[str]
    argtypes: t.List[str]
    return_type: str
    definition: str


class Trigger(te.TypedDict):
    obj_type: te.Literal["trigger"]
    oid: str
    schema: str
    name: str
    table_name: str
    definition: str
    proc_name: str
    proc_schema: str
    enabled: bool


class Dependency(te.TypedDict):
    obj_type: te.Literal["dependency"]
    oid: int
    identity: str
    dependency_oid: str
    dependency_identity: str


class Database(te.TypedDict):
    tables: t.Dict[str, Table]
    views: t.Dict[str, View]
    indices: t.Dict[str, Index]
    enums: t.Dict[str, Enum]
    sequences: t.Dict[str, Sequence]
    functions: t.Dict[str, Function]
    triggers: t.Dict[str, Trigger]
    dependencies: "nx.Graph"


DBObject = t.Union[
    Table,
    View,
    Index,
    Sequence,
    Enum,
    Function,
    Trigger,
]
