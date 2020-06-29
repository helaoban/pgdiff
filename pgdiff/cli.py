import sys
import click


@click.group()
def cli():
    pass

@cli.command()
@click.argument("dsn")
@click.option("--schemas", "-s", type=str, default="")
def sync(dsn, schemas: str):
    """Sync database @ [dsn] with schema."""
    from .sync import sync as do_sync
    schema = sys.stdin.read()
    include = schemas.split(" ") if schemas else None
    do_sync(schema, dsn, include)
