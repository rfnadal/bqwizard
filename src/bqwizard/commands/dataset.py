import click
from click import Context
from .utils.dataset_utils import (
    check_dataset_existence,
    create_view,
    create_dataset,
    create_dataset_chain,
    create_dataset_chain_views,
    describe_dataset,
)
from tabulate import tabulate
from google.api_core.exceptions import NotFound


@click.group()
@click.pass_context
def dataset(ctx: Context):
    """Manage BigQuery Datasets.

    Args:
        ctx: Click context object for managing shared state between commands
    """
    ctx.ensure_object(dict)


@dataset.command()
@click.argument("dataset_name")
@click.pass_context
def tables(ctx: Context, dataset_name: str) -> None:
    """List all tables in a specified BigQuery dataset.

    Args:
        ctx: Click context object containing project and client information
        dataset_name (str): Name of the dataset to list tables from

    Returns:
        str: Prints formatted table showing Table ID, Dataset, and Type information

    Raises:
        Exception: If there's an error accessing the dataset or listing tables
    """
    try:
        project = ctx.obj["PROJECT"]
        client = ctx.obj["CLIENT"]
        click.echo(f"Listing tables in dataset {dataset_name} of project {project}")
        dataset_ref = client.dataset(dataset_name, project=project)
        table_data = [
            (t.table_id, t.dataset_id, t.table_type)
            for t in client.list_tables(dataset_ref)
        ]
        click.echo(
            tabulate(
                table_data, headers=["Table ID", "Dataset", "Type"], tablefmt="grid"
            )
        )
    except Exception as e:
        click.echo(f"Error: {str(e)}")


@dataset.command()
@click.pass_context
def ls(ctx: Context) -> None:
    """List all datasets in the current project.

    Args:
        ctx: Click context object containing project and client information

    Returns:
        str: Prints formatted table of Dataset IDs

    Raises:
        NotFound: If the specified project is not found
    """
    try:
        project = ctx.obj["PROJECT"]
        client = ctx.obj["CLIENT"]
        datasets = list(client.list_datasets())
        if datasets:
            click.echo(f"Listing datasets in project {project}.")
            dataset_data = [[d.dataset_id] for d in datasets]
            click.echo(tabulate(dataset_data, headers=["Dataset ID"], tablefmt="grid"))
        else:
            print(f"{project} does not contain any datasets.")
    except NotFound:
        click.echo(f"Project {project} not found.")


@dataset.command()
@click.pass_context
def describe_all(ctx: Context) -> None:
    """List detailed information about all datasets and their corresponding tables.

    Args:
        ctx: Click context object containing project and client information

    Returns:
        None: Prints detailed information about each dataset and its tables
    """
    client = ctx.obj["CLIENT"]
    project = ctx.obj["PROJECT"]
    datasets = list(client.list_datasets())
    for dataset_item in datasets:
        dataset = client.get_dataset(dataset_item.reference)
        describe_dataset(client, dataset, project)


@dataset.command()
@click.pass_context
@click.argument("dataset_name")
@click.option("--location", default="US")
def create(ctx: Context, dataset_name: str, location: str) -> None:
    """Create a new BigQuery dataset in the specified location.

    Args:
        ctx: Click context object containing project and client information
        dataset_name (str): Name of the dataset to create
        location (str): Geographic location for the dataset (default: "US")

    Returns:
        None: Prints success message upon creation

    Raises:
        Exception: If dataset creation fails
    """
    client = ctx.obj["CLIENT"]
    project = ctx.obj["PROJECT"]
    if project:
        try:
            dataset_ref = f"{project}.{dataset_name}"
            confirmation = click.confirm(
                f"Create dataset {dataset_ref} in location {location}?"
            )
            if confirmation:
                click.echo(f"Creatiing dataset {dataset_ref} in location {location}")
                dataset = client.create_dataset(client, dataset_ref, timeout=30)
                click.echo(
                    f"Successfully created dataset {dataset_ref} in location {location}"
                )
        except Exception as e:
            click.echo(f"Unknow Exception Occured: {e}")
    else:
        click.echo(
            "Please either pass a project id or set the GOOGLE_CLOUD_PROJECT environment variable."
        )


@dataset.command()
@click.pass_context
@click.argument("dataset_name")
def delete(ctx: Context, dataset_name: str) -> None:
    """Delete a BigQuery dataset and all its contents.

    Args:
        ctx: Click context object containing project and client information
        dataset_name (str): Name of the dataset to delete. Can include project ID (project.dataset)

    Returns:
        None: Prints success message upon deletion

    Raises:
        Exception: If dataset deletion fails

    Note:
        Requires double confirmation due to destructive nature of operation
    """
    client = ctx.obj["CLIENT"]
    project = ctx.obj["PROJECT"]
    try:
        if len(dataset_name.split(".")[0]) > 0:
            project = dataset_name.split(".")[0]
            dataset_name = dataset_name.split(".")[1]
        elif len(dataset_name.split(".")[0]) == 0 and project is None:
            click.echo(
                "Please either pass a fully qualified dataset name or set the GOOGLE_CLOUD_PROJECT environment variable."
            )
            return None
        if project:
            dataset_ref = f"{project}.{dataset_name}"
            confirmation_1 = click.confirm(f"Delete dataset {dataset_ref}?")
            confirmation_2 = click.confirm("This is a distructive action are you sure?")
            if confirmation_1 and confirmation_2:
                client.delete_dataset(
                    dataset_ref, delete_contents=True, not_found_ok=True
                )
                click.echo(f"Successfully deleted the {dataset_ref} dataset.")
            else:
                click.echo("Deletion aborted")
        else:
            click.echo(
                "Please either pass a project id or set the GOOGLE_CLOUD_PROJECT environment variable."
            )
    except Exception as e:
        click.echo(f"Unknown error occured: {e}")


@dataset.command()
@click.argument("source_project")
@click.argument("source_dataset")
@click.argument("target_project")
@click.argument("target_dataset")
@click.option(
    "--force",
    help="Automatically create target datasets if they don't exist.",
    is_flag=True,
)
@click.pass_context
def expose(
    ctx: Context,
    source_project: str,
    source_dataset: str,
    target_project: str,
    target_dataset: str,
    force: bool,
) -> None:
    """Create views in a target dataset that reference tables from a source dataset.

    Args:
        ctx: Click context object containing client information
        source_project (str): Project ID containing the source dataset
        source_dataset (str): Name of the source dataset
        target_project (str): Project ID where views will be created
        target_dataset (str): Name of the target dataset for views
        force (bool): If True, creates target dataset if it doesn't exist

    Returns:
        None: Prints progress and completion messages

    Raises:
        Exception: If view creation fails
    """
    try:
        client = ctx.obj["CLIENT"]
        source_dataset_ref = f"{source_project}.{source_dataset}"
        target_dataset_ref = f"{target_project}.{target_dataset}"
        click.echo(f"Exposing dataset {source_dataset_ref} to {target_dataset_ref}")
        if check_dataset_existence(
            client, source_dataset_ref
        ) and check_dataset_existence(client, target_dataset_ref):
            for tables in client.list_tables(source_dataset_ref):
                create_view(
                    client, source_dataset_ref, target_dataset_ref, tables.table_id
                )
        elif not check_dataset_existence(client, target_dataset_ref) and force:
            click.echo(f"Creating missing dataset {target_dataset_ref}.")
            create_dataset(client, target_dataset_ref)
            for tables in client.list_tables(source_dataset_ref):
                create_view(
                    client, source_dataset_ref, target_dataset_ref, tables.table_id
                )
        else:
            click.echo(
                "Error: Please make sure that source and target datasets exists."
            )
        click.echo("Done.")
    except Exception as e:
        click.echo(f"Unknown Exception Occured: {e}")


@dataset.command
@click.argument("datasets", nargs=-1, type=str)
@click.option(
    "--force",
    help="Automatically create target datasets if they don't exist.",
    is_flag=True,
)
@click.pass_context
def chain(ctx: Context, datasets: tuple, force: bool) -> None:
    """Create a chain of datasets with views referencing tables from the previous dataset.

    Args:
        ctx: Click context object containing client information
        datasets (tuple): Ordered sequence of dataset names to chain together
        force (bool): If True, creates missing datasets automatically

    Returns:
        None: Prints completion message when chain is created

    Example:
        If datasets = ('dataset1', 'dataset2', 'dataset3'), creates:
        - Views in dataset2 pointing to dataset1's tables
        - Views in dataset3 pointing to dataset2's views
    """
    client = ctx.obj["CLIENT"]
    datasets_exist = all(
        [check_dataset_existence(client, dataset) for dataset in datasets]
    )
    if datasets_exist:
        create_dataset_chain_views(client, datasets)
    elif not datasets_exist and force:
        create_dataset_chain(client, datasets)
        create_dataset_chain_views(client, datasets)
    else:
        click.echo("Not all target dataset's exist. Either create them or use --force.")
    click.echo("Chain completed.")


@dataset.command
@click.argument("dataset")
@click.pass_context
def describe(ctx: Context, dataset: str) -> None:
    """Display detailed information about a specific dataset and its tables.

    Args:
        ctx: Click context object containing project and client information
        dataset: Dataset reference to describe

    Returns:
        None: Prints detailed dataset information including:
            - Dataset ID
            - Description
            - Location
            - Labels
            - Table listing with details
    """
    client = ctx.obj["CLIENT"]
    project = ctx.obj["PROJECT"]
    dataset = client.get_dataset(dataset)
    describe_dataset(client, dataset, project)
