import click
from click import Context
from tabulate import tabulate


@click.group()
@click.pass_context
def table(ctx: Context):
    """
    A group of commands for managing tables.
    """
    pass


@table.command()
@click.argument("table")
@click.pass_context
def describe(ctx: Context, table: str):
    """
    Describes a table and its metadata.

    Args:
        table: The table to describe.
    Returns:
        None: A table description.
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    if len(table.split(".")) <= 1:
        click.echo("Please provide the table in the following format: dataset.table")
        raise click.Abort()
    table_id = f"{project}.{table}"
    table_ref = client.get_table(table_id)
    table_data = [
        [
            table_ref.created.strftime("%B %d, %Y at %I:%M %p UTC"),
            table_ref.modified.strftime("%B %d, %Y at %I:%M %p UTC"),
            f"{table_ref.num_rows:,}",
            table_ref.table_type,
            f"{table_ref.num_bytes / 1000000:.2f} MB",
            table_ref.partitioning_type or "None",
            ", ".join(table_ref.clustering_fields)
            if table_ref.clustering_fields
            else "None",
        ]
    ]
    click.echo(f"\nDisplaying the data for: {table_id} \n")
    click.echo(
        tabulate(
            table_data,
            headers=[
                "Created",
                "Modified",
                "Rows",
                "Type",
                "Size",
                "Partition",
                "Cluster",
            ],
            tablefmt="simple",
        )
    )
    click.echo("\n")
