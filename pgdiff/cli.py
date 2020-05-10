import sys
import click


@click.group()
def cli():
    pass

@cli.command()
@click.argument("dsn")
def sync(dsn):
    """Sync database @ [dsn] with schema."""
    from .sync import sync as do_sync
    schema = sys.stdin.read()
    do_sync(schema, dsn)
