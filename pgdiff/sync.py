import contextlib
import sys
import typing as t

from .diff import diff
from .inspect import inspect
from .utils import temp_db, quick_cursor

from psycopg2 import connect as db_connect  # type: ignore
from psycopg2.extras import RealDictCursor  # type: ignore


def _wrap(statements: t.Iterable[str], rollback: bool = False) -> str:
    end = "ROLLBACK" if rollback else "COMMIT"
    script = "SET check_function_bodies = false;\n\n"
    script += "BEGIN;\n\n%s\n\n%s;" % ("\n\n".join(statements), end)
    return script


def sync(
    schema: str,
    dsn: str,
    schemas: t.Optional[t.List[str]] = None,
    dry_run: bool = True
) -> None:
    with contextlib.ExitStack() as stack:
        temp_db_dsn = stack.enter_context(temp_db(dsn))
        target = stack.enter_context(quick_cursor(temp_db_dsn, RealDictCursor))
        current = stack.enter_context(quick_cursor(dsn, RealDictCursor))
        target.execute(schema)
        target_schema = inspect(target, include=schemas)
        current_schema = inspect(current, include=schemas)

    statements = target_schema.diff(current_schema)
    if statements:
        sys.stdout.write(_wrap(statements, rollback=dry_run))
