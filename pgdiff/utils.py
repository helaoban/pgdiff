import random
import string
import typing as t
import contextlib
import os
import urllib.parse
from copy import copy

from psycopg2 import connect as db_connect, sql  # type: ignore
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # type: ignore
from psycopg2.errors import InsufficientPrivilege  # type: ignore
from time import sleep


KILL_CONN = """
    SELECT pg_terminate_backend(pid) FROM pg_stat_activity
    WHERE datname = %s
        AND pid != pg_backend_pid();
"""


class DBConnParams:

    def __init__(
        self,
        username: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
        database: str = "",
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def to_dsn(self):
        return (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}"
            f"/{self.database}"
        )


def _validate_port_spec(hosts, port):
    if isinstance(port, list):
        # If there is a list of ports, its length must
        # match that of the host list.
        if len(port) != len(hosts):
            raise ValueError(
                'could not match {} port numbers to {} hosts'.format(
                    len(port), len(hosts)))
    else:
        port = [port for _ in range(len(hosts))]

    return port


def _parse_hostlist(hostlist, port, unquote=False):
    if ',' in hostlist:
        # A comma-separated list of host addresses.
        hostspecs = hostlist.split(",")
    else:
        hostspecs = [hostlist]

    hosts = []
    hostlist_ports = []

    if not port:
        portspec = os.environ.get("PGPORT")
        if portspec:
            if "," in portspec:
                default_port = [int(p) for p in portspec.split(",")]
            else:
                default_port = int(portspec)
        else:
            default_port = 5432

        default_port = _validate_port_spec(hostspecs, default_port)

    else:
        port = _validate_port_spec(hostspecs, port)

    for i, hostspec in enumerate(hostspecs):
        if not hostspec.startswith("/"):
            addr, _, hostspec_port = hostspec.partition(":")
        else:
            addr = hostspec
            hostspec_port = ""

        if unquote:
            addr = urllib.parse.unquote(addr)

        hosts.append(addr)
        if not port:
            if hostspec_port:
                if unquote:
                    hostspec_port = urllib.parse.unquote(hostspec_port)
                hostlist_ports.append(int(hostspec_port))
            else:
                hostlist_ports.append(default_port[i])

    if not port:
        port = hostlist_ports

    return hosts, port


def parse_db_dsn(dsn: str) -> DBConnParams:
    parsed = urllib.parse.urlparse(dsn)

    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError(
            "invalid DSN: scheme is expected to be either "
            "'postgresql' or 'postgres', got {!r}".format(parsed.scheme))

    if parsed.netloc:
        if "@" in parsed.netloc:
            dsn_auth, _, dsn_hostspec = parsed.netloc.partition("@")
        else:
            dsn_hostspec = parsed.netloc
            dsn_auth = ""
        hosts, ports = _parse_hostlist(dsn_hostspec, None, unquote=True)
        host = hosts[0]
        port = ports[0]
    else:
        dsn_auth = dsn_hostspec = ""
        host, port = "localhost", 5432

    if dsn_auth:
        dsn_user, _, dsn_password = dsn_auth.partition(":")
        user = urllib.parse.unquote(dsn_user)
        password = urllib.parse.unquote(dsn_password)
    else:
        user = password = ""

    if not host and dsn_hostspec:
        host, port = _parse_hostlist(dsn_hostspec, None, unquote=True)

    if parsed.path:
        dsn_database = parsed.path
        if dsn_database.startswith("/"):
            dsn_database = dsn_database[1:]
        database = urllib.parse.unquote(dsn_database)
    else:
        database = ""

    if parsed.query:
        _query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        query = {}
        for k, v in _query.items():
            if isinstance(v, list):
                query[k] = v[-1]

        if 'port' in query:
            val = query.pop('port')
            if not port and val:
                port = [int(p) for p in val.split(',')]

        if 'host' in query:
            val = query.pop('host')
            if not host and val:
                host, port = _parse_hostlist(val, port)

        if 'database' in query:
            val = query.pop('database')
            if database is None:
                database = val

        if 'user' in query:
            val = query.pop('user')
            if user is None:
                user = val

        if 'password' in query:
            val = query.pop('password')
            if password is None:
                password = val

    return DBConnParams(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
    )


def database_exists(conn, name):
    cur = conn.execute()
    result = cur.execute(
        """
        SELECT 1
        FROM pg_catalog.pg_database
        WHERE datname = %s
        """,
        name
    )
    return bool(result)


def _temporary_name(prefix="tmp_"):
    random_letters = [
        random.choice(string.ascii_lowercase)
        for _ in range(10)
    ]
    rnd = "".join(random_letters)
    return prefix + rnd


def get_raw_connection(dsn: str):
    conn = db_connect(dsn)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def admin_connect(dsn):
    params = copy(parse_db_dsn(dsn))
    params.database = "postgres"
    return get_raw_connection(params.to_dsn())


def kill_other_connections(conn, db_name: str):
    cur = conn.cursor()
    cur.execute(
        sql.SQL("REVOKE CONNECT ON DATABASE {} FROM public").format(
            sql.Identifier(db_name))
    )
    tries = 5
    for tri in range(1, tries + 1):
        try:
            cur.execute(KILL_CONN, (db_name, ))
        except InsufficientPrivilege:
            if tri == tries:
                raise
            sleep(0.1)
            continue
        else:
            break


def create_database(dsn, template: str = ""):
    params = parse_db_dsn(dsn)
    conn = admin_connect(dsn)
    cur = conn.cursor()
    if template:
        cur.execute(
            sql.SQL(
                "CREATE DATABASE {} TEMPLATE {}"
            ).format(
                sql.Identifier(params.database),
                sql.Identifier(template),
            )
        )
    else:
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(params.database),
            )
        )
    conn.close()


def drop_database(dsn):
    params = parse_db_dsn(dsn)
    conn = admin_connect(dsn)
    kill_other_connections(conn, params.database)
    cur = conn.cursor()
    cur.execute(
        sql.SQL("DROP DATABASE IF EXISTS {}").format(
            sql.Identifier(params.database))
    )
    conn.close()


@contextlib.contextmanager
def temp_db(dsn: str, template: str = "", quiet: bool = True):
    params = copy(parse_db_dsn(dsn))
    params.database = _temporary_name()
    temp_db_dsn = params.to_dsn()
    if not quiet:
        print(f"Creating temporary database {params.database}.")
    create_database(temp_db_dsn, template=template)
    try:
        yield temp_db_dsn
    finally:
        if not quiet:
            print(f"Dropping temporary database {params.database}.")
        drop_database(temp_db_dsn)


@contextlib.contextmanager
def quick_cursor(dsn, cursor_factory = None):
    conn = db_connect(dsn)
    if cursor_factory is not None:
        cur = conn.cursor(cursor_factory=cursor_factory)
    else:
        cur = conn.cursor()
    try:
        yield cur
    finally:
        conn.close()
