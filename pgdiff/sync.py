import contextlib
import sys
import typing as t

from .diff import diff
from .inspect import inspect
from .utils import temp_db, quick_cursor

from psycopg2 import connect as db_connect  # type: ignore
from psycopg2.extras import RealDictCursor  # type: ignore


def wrap_in_transaction(statements: t.List[str]) -> str:
    script = "SET check_function_bodies = false;\n\n"
    script += "BEGIN;\n\n%s\n\nCOMMIT;" % "\n\n".join(statements)
    return script


def sync(schema: str, dsn: str, schemas: t.Optional[t.List[str]] = None) -> None:
    with temp_db(dsn) as temp_db_dsn:
        with contextlib.ExitStack() as stack:
            target = stack.enter_context(quick_cursor(temp_db_dsn, RealDictCursor))
            current = stack.enter_context(quick_cursor(dsn, RealDictCursor))
            target.execute(schema)
            target_schema = inspect(target, include=schemas)
            current_schema = inspect(current, include=schemas)
            statements = target_schema.diff(current_schema)
            if statements:
                script = wrap_in_transaction(statements)
                sys.stdout.write(script)
