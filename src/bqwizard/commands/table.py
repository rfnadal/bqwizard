import click
from click import Context
from tabulate import tabulate
from .utils.table_utils import validate_table_id, write_to_csv, create_view
from google.api_core.exceptions import NotFound


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
    validate_table_id(table)
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


def chain():
    pass


@table.command()
@click.argument("table")
@click.pass_context
def delete(ctx: Context, table: str):
    """
    Delete a specific table
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    validate_table_id(table)
    table_id = f"{project}.{table}"
    confirmation_1 = click.confirm(f"Would you like to delete table {table_id}?")
    confirmation_2 = click.confirm("Are you sure?")
    if confirmation_1 and confirmation_2:
        client.delete_table(table_id)
        click.echo(f"Table {table_id} delete successfully.")
    else:
        click.Abort()


@table.command()
@click.argument("table")
@click.pass_context
def refresh_view(ctx: Context, table: str):
    """
    Recreates a view to refresh any columns that might have changed.
    Args:
    ctx (Context): Click Context Object.
    table (str): Table/View we want to recreate.
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    validate_table_id(table)
    view_id = f"{project}.{table}"
    try:
        table_ref = client.get_table(view_id)
        if table_ref.table_type == "VIEW":
            view_sql = table_ref.view_query
            refresh_query = f"""

            CREATE OR REPLACE VIEW {view_id} as {view_sql}

            """
            refresh_view_query = client.query(refresh_query)
            refresh_view_query.result()
            click.echo(f"View {view_id} table created successfuully.")
        else:
            click.echo("Table {view_id} is not a view ")
    except NotFound:
        click.echo(f"Table {table} not found.")


@table.command()
@click.argument("table")
@click.option(
    "--rows",
    "-r",
    default=5,
    help="Number of rows to display (default: 5)",
    type=int,
)
@click.pass_context
def head(ctx: Context, table: str, rows: int):
    """
    Display the first few rows of a table.

    Args:
        table: The table to display rows from
        rows: Number of rows to display (default: 5)
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    validate_table_id(table)
    table_id = f"{project}.{table}"

    try:
        table_ref = client.get_table(table_id)
        columns = [
            field.name for field in table_ref.schema if not field.name.startswith("_")
        ]

        columns_str = ", ".join(f"`{col}`" for col in columns)
        query = f"""
        SELECT {columns_str}
        FROM `{table_id}`
        ORDER BY 1
        LIMIT {rows}
        """

        query_job = client.query(query)
        results = query_job.result()

        rows_data = [list(row.values()) for row in results]

        click.echo(f"\nFirst {rows} rows of table: {table_id}\n")
        click.echo(
            tabulate(
                rows_data,
                headers=columns,
                tablefmt="simple",
                numalign="left",
                stralign="left",
            )
        )
        click.echo("\n")
    except NotFound:
        click.echo(f"Table {table_id} not found.")
    except Exception as e:
        click.echo(f"Error displaying table rows: {str(e)}")


@table.command()
@click.argument("table")
@click.option(
    "--percent",
    "-p",
    default=5,
    help="Percent of table used for sampling (default: 100)",
    type=int,
)
@click.option("-f", "--filename", "dest", default="sample.csv")
@click.pass_context
def sample(ctx: Context, table: str, percent: int, dest: str) -> None:
    """
    Export sample rows from a table.

    Args:
        ctx (Context): Click Context Object.
        table (str): The full qualified name of the target table.
        percent (int): The percent of the table to use for sampling.
        dest: (str): The CSV file to which you want to store the smaple data.
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    if not dest.endswith(".csv"):
        raise click.BadOptionUsage(
            option_name="filename",
            message="Please provide a filename that ends with .csv",
        )
    validate_table_id(table)
    table_id = f"{project}.{table}"
    sample_query = f"""

    SELECT * FROM `{table_id}` TABLESAMPLE SYSTEM ({percent} PERCENT) LIMIT 100
    
    """
    sample_query_job = client.query(sample_query)
    sample_query_results = sample_query_job.result()
    write_to_csv(sample_query_results, dest)
    click.echo(f"Sample of {table} file exported to {dest}")


@table.command()
@click.argument("source_table")
@click.argument("target_table")
@click.option(
    "--force",
    is_flag=True,
    help="Automatically create target dataset if it does not exist.",
)
@click.pass_context
def expose(ctx: Context, source_table: str, target_table: str, force: bool) -> None:
    """
    Creates a view of a table

    Args:
    ctx (Context): Click Context Object.
    source_table (str): The full table_id for the source table to create the view from.
    target_table (str): The full table_id for the target table to create the view from.

    Returns:
        None
    """
    client = ctx.obj["CLIENT"]
    create_view(client, source_table, target_table, force)


# TODO:
