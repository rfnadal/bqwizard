import click
from click import Context
from tabulate import tabulate
from .utils.table_utils import (
    write_to_csv,
    create_view,
)
from .utils.reference_utils import (
    get_table_id,
    validate_table_id,
)
from .utils.chain_utils import ChainOperationTracker
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
        table: The table to describe. Can be specified as dataset.table or project.dataset.table
    Returns:
        None: A table description.
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validate the table reference before processing
    validate_table_id(table, "short")
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
def delete(ctx: Context, table: str):
    """
    Delete a specific table

    Args:
        table: The table to delete. Can be specified as dataset.table or project.dataset.table
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validate the table reference before processing
    validate_table_id(table, "short")
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
def refresh_view(ctx: Context, table: str):
    """
    Recreates a view to refresh any columns that might have changed.

    Args:
        ctx (Context): Click Context Object.
        table (str): Table/View we want to recreate. Can be specified as dataset.table or project.dataset.table
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validate the table reference before processing
    validate_table_id(table, "short")
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
    default=5,
    help="Number of rows to display (default: 5)",
    type=int,
)
@click.pass_context
def head(ctx: Context, table: str, rows: int):
    """
    Display the first few rows of a table.

    Args:
        table: The table to display rows from. Can be specified as dataset.table or project.dataset.table
        rows: Number of rows to display (default: 5)
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validate the table reference before processing
    validate_table_id(table, "short")
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

    # Validate the table reference before processing
    validate_table_id(table, "short")
    table_id = get_table_id(project, table)
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
        source_table (str): The source table to create the view from. Can be specified as dataset.table or project.dataset.table
        target_table (str): The target table to create. Can be specified as dataset.table or project.dataset.table

    Returns:
        None
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validate both table references before processing
    validate_table_id(source_table, "short")
    validate_table_id(target_table, "short")
    source_table_id = get_table_id(project, source_table)
    target_table_id = get_table_id(project, target_table)

    create_view(client, source_table_id, target_table_id, force)


@table.command()
@click.argument("view")
@click.pass_context
def genealogy(ctx: Context, view: str):
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validate the table reference before processing
    validate_table_id(view, "short")
    source_table_id = get_table_id(project, view)
    click.echo(source_table_id)


@table.command()
@click.argument("source_tables", nargs=-1, required=True)
@click.argument("target_datasets", nargs=-1, required=True)
@click.option(
    "--dry-run",
    help="Preview what will be created without actually creating anything.",
    is_flag=True,
)
@click.option(
    "--force",
    help="Automatically create target datasets if they don't exist.",
    is_flag=True,
)
@click.pass_context
def chain(ctx: Context, source_tables: tuple, target_datasets: tuple, dry_run: bool, force: bool) -> None:
    """Create views of specific tables across multiple datasets.

    This command creates views of the specified source tables in each target dataset.
    Unlike dataset chain which chains all tables from one dataset to the next,
    this command creates the same tables (as views) across multiple target datasets.

    Args:
        source_tables: List of table references to chain (dataset.table or project.dataset.table)
        target_datasets: List of target datasets where views will be created
        dry_run: Preview what would be created without creating anything
        force: Create target datasets if they don't exist

    Examples:
        # Create views of specific tables in multiple datasets
        bqwizard table chain dataset1.table1 dataset1.table2 dataset2 dataset3

        # With project qualification
        bqwizard table chain project1.dataset1.table1 project2.dataset2 project3.dataset3
    """
    project = ctx.obj["PROJECT"]
    client = ctx.obj["CLIENT"]

    # Validation
    if len(source_tables) == 0:
        click.echo("Error: At least one source table is required.", err=True)
        ctx.exit(1)

    if len(target_datasets) == 0:
        click.echo("Error: At least one target dataset is required.", err=True)
        ctx.exit(1)

    if dry_run:
        click.echo("DRY RUN MODE - No changes will be made")
        click.echo("=" * 50)

    # Validate and qualify all source tables
    qualified_source_tables = []
    for table_ref in source_tables:
        try:
            validate_table_id(table_ref, "short")
            qualified_table = get_table_id(project, table_ref)
            qualified_source_tables.append(qualified_table)
        except Exception as e:
            click.echo(f"Invalid source table '{table_ref}': {e}", err=True)
            ctx.exit(1)

    # Validate and qualify all target datasets
    qualified_target_datasets = []
    for dataset_ref in target_datasets:
        try:
            from .utils.dataset_utils import get_dataset_id
            qualified_dataset = get_dataset_id(client, dataset_ref)
            qualified_target_datasets.append(qualified_dataset)
        except Exception as e:
            click.echo(f"Invalid target dataset '{dataset_ref}': {e}", err=True)
            ctx.exit(1)

    # Check target dataset existence
    from .utils.dataset_utils import check_dataset_existence, create_dataset

    datasets_to_create = []
    for dataset_id in qualified_target_datasets:
        if not check_dataset_existence(client, dataset_id):
            if force:
                datasets_to_create.append(dataset_id)
                if dry_run:
                    click.echo(f"Would create dataset: {dataset_id}")
                else:
                    try:
                        create_dataset(client, dataset_id)
                        click.echo(f"Created dataset: {dataset_id}")
                    except Exception as e:
                        click.echo(f"Failed to create dataset {dataset_id}: {e}", err=True)
                        ctx.exit(1)
            else:
                if dry_run:
                    click.echo(f"Target dataset {dataset_id} does not exist and would need --force to create.")
                else:
                    click.echo(f"Target dataset {dataset_id} does not exist. Use --force to create it.", err=True)
                ctx.exit(1)

    # Create table chain
    tracker = ChainOperationTracker(client, dry_run)

    if dry_run:
        click.echo(f"\nCreating views for {len(qualified_source_tables)} tables in {len(qualified_target_datasets)} datasets:")
        click.echo("-" * 60)

    success = True
    total_operations = len(qualified_source_tables) * len(qualified_target_datasets)
    completed_operations = 0

    try:
        for source_table_id in qualified_source_tables:
            # Extract table name from source
            table_name = source_table_id.split(".")[-1]

            for target_dataset_id in qualified_target_datasets:
                if not tracker.create_view_with_tracking(
                    source_table_id, target_dataset_id, table_name
                ):
                    success = False
                    break
                completed_operations += 1

            if not success:
                break

    except Exception as e:
        click.echo(f"Unexpected error during table chain creation: {e}", err=True)
        success = False

    # Print results
    tracker.print_operation_summary()

    if dry_run:
        if success:
            click.echo(f"\nDry run completed successfully. Would create {total_operations} views.")
            click.echo("Use without --dry-run to execute.")
        else:
            click.echo(f"\nDry run found issues that would prevent execution.", err=True)
    else:
        if success:
            click.echo(f"Table chain completed successfully. Created {completed_operations} views.")
        else:
            click.echo(f"Table chain operation failed after {completed_operations}/{total_operations} operations.", err=True)
