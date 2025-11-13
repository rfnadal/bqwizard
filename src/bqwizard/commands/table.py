import click
from tabulate import tabulate
from .utils.table_utils import (
    write_to_csv,
    get_table_id,
)
from .utils.dataset_utils import (
    create_view,
    check_dataset_existence,
    create_dataset,
)
from google.api_core.exceptions import NotFound, GoogleAPIError

# Constants
DEFAULT_HEAD_ROWS = 5
DEFAULT_SAMPLE_PERCENT = 5
DEFAULT_SAMPLE_LIMIT = 100


@click.group()
@click.pass_context
def table(ctx: click.Context):
    """
    A group of commands for managing tables.
    """
    pass


@table.command()
@click.argument("table")
@click.pass_context
def describe(ctx: click.Context, table: str):
    """
    Describes a table and its metadata.

    Args:
        table: The table to describe. Can be specified as dataset.table or project.dataset.table
    Returns:
        None: A table description.
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    table_id = get_table_id(project, table)
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



@table.command()
@click.argument("table")
@click.pass_context
def delete(ctx: click.Context, table: str):
    """
    Delete a specific table

    Args:
        table: The table to delete. Can be specified as dataset.table or project.dataset.table
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    table_id = get_table_id(project, table)
    confirmation_1 = click.confirm(f"Would you like to delete table {table_id}?")
    confirmation_2 = click.confirm("Are you sure?")
    if confirmation_1 and confirmation_2:
        client.delete_table(table_id)
        click.echo(f"Table {table_id} deleted successfully.")
    else:
        click.Abort()


@table.command()
@click.argument("table")
@click.pass_context
def refresh_view(ctx: click.Context, table: str):
    """
    Recreates a view to refresh any columns that might have changed.

    Args:
        ctx (Context): Click Context Object.
        table (str): Table/View we want to recreate. Can be specified as dataset.table or project.dataset.table
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    view_id = get_table_id(project, table)

    try:
        table_ref = client.get_table(view_id)
        if table_ref.table_type == "VIEW":
            view_sql = table_ref.view_query
            refresh_query = f"""

            CREATE OR REPLACE VIEW `{view_id}` as {view_sql}

            """
            refresh_view_query = client.query(refresh_query)
            refresh_view_query.result()
            click.echo(f"View {view_id} created successfully.")
        else:
            click.echo(f"Table {view_id} is not a view")
    except NotFound:
        click.echo(f"Table {view_id} not found.")


@table.command()
@click.argument("table")
@click.option(
    "--rows",
    "-r",
    default=DEFAULT_HEAD_ROWS,
    help=f"Number of rows to display (default: {DEFAULT_HEAD_ROWS})",
    type=int,
)
@click.pass_context
def head(ctx: click.Context, table: str, rows: int):
    """
    Display the first few rows of a table.

    Args:
        table: The table to display rows from. Can be specified as dataset.table or project.dataset.table
        rows: Number of rows to display (default: 5)
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    table_id = get_table_id(project, table)

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
    except GoogleAPIError as e:
        click.echo(f"BigQuery API error: {str(e)}")


@table.command()
@click.argument("table")
@click.option(
    "--percent",
    "-p",
    default=DEFAULT_SAMPLE_PERCENT,
    help=f"Percent of table used for sampling (default: {DEFAULT_SAMPLE_PERCENT})",
    type=int,
)
@click.option("-f", "--filename", "dest", default="sample.csv")
@click.pass_context
def sample(ctx: click.Context, table: str, percent: int, dest: str) -> None:
    """
    Export sample rows from a table.

    Args:
        ctx (Context): Click Context Object.
        table (str): The table to sample from. Can be specified as dataset.table or project.dataset.table
        percent (int): The percent of the table to use for sampling.
        dest: (str): The CSV file to which you want to store the sample data.
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]
    if not dest.endswith(".csv"):
        raise click.BadOptionUsage(
            option_name="filename",
            message="Please provide a filename that ends with .csv",
        )
    table_id = get_table_id(project, table)
    sample_query = f"""

    SELECT * FROM `{table_id}` TABLESAMPLE SYSTEM ({percent} PERCENT) LIMIT {DEFAULT_SAMPLE_LIMIT}

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
def expose(ctx: click.Context, source_table: str, target_table: str, force: bool) -> None:
    """
    Creates a view of a table

    Args:
        ctx (Context): Click Context Object.
        source_table (str): The source table to create the view from. Can be specified as dataset.table or project.dataset.table
        target_table (str): The target table to create. Can be specified as dataset.table or project.dataset.table

    Returns:
        None
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    source_table_id = get_table_id(project, source_table)
    target_table_id = get_table_id(project, target_table)

    # Parse target_table_id to extract dataset and table name
    target_parts = target_table_id.split(".")
    if len(target_parts) == 3:
        target_project, target_dataset, target_table_name = target_parts
        target_dataset_ref = f"{target_project}.{target_dataset}"
    else:
        raise click.BadParameter(f"Invalid target table format: {target_table_id}")

    # Check if target dataset exists, create if force is True
    if not check_dataset_existence(client, target_dataset_ref):
        if force:
            create_dataset(client, target_dataset_ref)
        else:
            click.echo("Target dataset does not exist. Either create it or use --force.")
            return

    create_view(client, source_table_id, target_dataset_ref, target_table_name)


