import contextlib
import sys
from .diff import diff
from .inspect import inspect
from .utils import temp_db, quick_cursor


from psycopg2 import connect as db_connect  # type: ignore
from psycopg2.extras import RealDictCursor  # type: ignore


def sync(schema: str, dsn: str):
    with temp_db(dsn) as temp_db_dsn:
        with contextlib.ExitStack() as stack:
            target = stack.enter_context(quick_cursor(temp_db_dsn, RealDictCursor))
            current = stack.enter_context(quick_cursor(dsn, RealDictCursor))
            target.execute(schema)
            target_schema = inspect(target)
            current_schema = inspect(current)
            statements = target_schema.diff(current_schema)
            if statements:
                script = "SET check_function_bodies = false;\n\n"
                script += "BEGIN;\n\n%s\n\nCOMMIT;" % "\n\n".join(statements)
                sys.stdout.write(script)
