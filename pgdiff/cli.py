import sys
import click


@click.group()
def cli() -> None:
    pass

@cli.command()
@click.argument("dsn", type=str)
@click.option("--schemas", "-s", type=str, default="")
@click.option("--dry", "-d", is_flag=True)
def sync(
    dsn: str,
    schemas: str,
    dry: bool,
) -> None:
    """Sync database @ [dsn] with schema."""
    from .sync import sync as do_sync
    schema = sys.stdin.read()
    include = schemas.split(" ") if schemas else None
    do_sync(
        schema,
        dsn,
        schemas=include,
        dry_run=dry,
    )
