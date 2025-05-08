import click
from click import Context
from .utils.dataset_utils import (
    check_dataset_existence,
    create_view,
    create_dataset,
    create_dataset_chain,
    create_dataset_chain_views,
    describe_dataset,
    get_dataset_id,
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
    pass


@dataset.command()
@click.argument("dataset_name")
@click.pass_context
def tables(ctx: Context, dataset_name: str) -> None:
    """List all tables in a specified BigQuery dataset.

    Args:
        ctx: Click context object containing project and client information
        dataset_name (str): Name of the dataset to list tables from.
                           Can be specified as 'dataset' or 'project.dataset'

    Returns:
        str: Prints formatted table showing Table ID, Dataset, and Type information

    Raises:
        Exception: If there's an error accessing the dataset or listing tables
    """
    try:
        client = ctx.obj["CLIENT"]
        dataset_id = get_dataset_id(client, dataset_name)
        project, dataset_name = dataset_id.split(".")
        
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
        dataset_name (str): Name of the dataset to create. Can be specified as 'dataset' or 'project.dataset'
        location (str): Geographic location for the dataset (default: "US")

    Returns:
        None: Prints success message upon creation

    Raises:
        Exception: If dataset creation fails
    """
    client = ctx.obj["CLIENT"]
    
    try:
        dataset_ref = get_dataset_id(client, dataset_name)
        
        confirmation = click.confirm(
            f"Create dataset {dataset_ref} in location {location}?"
        )
        if confirmation:
            click.echo(f"Creating dataset {dataset_ref} in location {location}")
            dataset = client.create_dataset(dataset_ref, location=location, timeout=30)
            click.echo(
                f"Successfully created dataset {dataset_ref} in location {location}"
            )
    except Exception as e:
        click.echo(f"Unknown Exception Occurred: {e}")


@dataset.command()
@click.pass_context
@click.argument("dataset_name")
def delete(ctx: Context, dataset_name: str) -> None:
    """Delete a BigQuery dataset and all its contents.

    Args:
        ctx: Click context object containing project and client information
        dataset_name (str): Name of the dataset to delete. Can be specified as 'dataset' or 'project.dataset'

    Returns:
        None: Prints success message upon deletion

    Raises:
        Exception: If dataset deletion fails

    Note:
        Requires double confirmation due to destructive nature of operation
    """
    client = ctx.obj["CLIENT"]
    
    try:
        dataset_ref = get_dataset_id(client, dataset_name)
        
        confirmation_1 = click.confirm(f"Delete dataset {dataset_ref}?")
        confirmation_2 = click.confirm("This is a destructive action are you sure?")
        if confirmation_1 and confirmation_2:
            client.delete_dataset(
                dataset_ref, delete_contents=True, not_found_ok=True
            )
            click.echo(f"Successfully deleted the {dataset_ref} dataset.")
        else:
            click.echo("Deletion aborted")
    except Exception as e:
        click.echo(f"Unknown error occurred: {e}")


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
        
        source_exists = check_dataset_existence(client, source_dataset_ref)
        target_exists = check_dataset_existence(client, target_dataset_ref)
        
        if source_exists and target_exists:
            source_ds_ref = client.dataset(source_dataset, project=source_project)
            tables = list(client.list_tables(source_ds_ref))
            
            for table in tables:
                source_table_id = f"{source_dataset_ref}.{table.table_id}"
                create_view(client, source_table_id, target_dataset_ref, table.table_id)
        elif not target_exists and force:
            click.echo(f"Creating missing dataset {target_dataset_ref}.")
            create_dataset(client, target_dataset_ref)
            
            if source_exists:
                source_ds_ref = client.dataset(source_dataset, project=source_project)
                tables = list(client.list_tables(source_ds_ref))
                
                for table in tables:
                    source_table_id = f"{source_dataset_ref}.{table.table_id}"
                    create_view(client, source_table_id, target_dataset_ref, table.table_id)
            else:
                click.echo(f"Source dataset {source_dataset_ref} does not exist.")
        else:
            click.echo(
                "Error: Please make sure that source and target datasets exist."
            )
        click.echo("Done.")
    except Exception as e:
        click.echo(f"Unknown Exception Occurred: {e}")


@dataset.command()
@click.argument("datasets", nargs=-1, type=str)
@click.option(
    "--force",
    help="Automatically create target datasets if they don't exist.",
    is_flag=True,
)
@click.option(
    "--tables-csv",
    help="Path to a CSV file containing a single column of table names to include in the chain.",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
)
@click.pass_context
def chain(ctx: Context, datasets: tuple, force: bool, tables_csv: str = None) -> None:
    """Create a chain of datasets with views referencing tables from the previous dataset.

    Args:
        ctx: Click context object containing client information
        datasets (tuple): Ordered sequence of dataset names to chain together. 
                           Each can be specified as 'dataset' or 'project.dataset'
        force (bool): If True, creates missing datasets automatically
        tables_csv (str, optional): Path to a CSV file with a single column listing 
                                   table names to include. Tables not in this list will be ignored.

    Returns:
        None: Prints completion message when chain is created

    Example:
        If datasets = ('dataset1', 'dataset2', 'dataset3'), creates:
        - Views in dataset2 pointing to dataset1's tables
        - Views in dataset3 pointing to dataset2's views
    """
    client = ctx.obj["CLIENT"]
    
    # Load tables from CSV if provided
    tables_to_include = None
    if tables_csv:
        try:
            import csv
            with open(tables_csv, 'r') as csvfile:
                reader = csv.reader(csvfile)
                tables_to_include = set(row[0] for row in reader if row)
            click.echo(f"Loaded {len(tables_to_include)} tables from {tables_csv}")
        except Exception as e:
            click.echo(f"Error reading CSV file: {e}")
            return
   
    qualified_datasets = []
    for ds in datasets:
        qualified_ds = get_dataset_id(client, ds)
        qualified_datasets.append(qualified_ds)
    
    datasets_exist = True
    for ds in qualified_datasets:
        if not check_dataset_existence(client, ds):
            datasets_exist = False
            if force:
                try:
                    create_dataset(client, ds)
                    click.echo(f"Created dataset {ds}")
                except Exception as e:
                    click.echo(f"Failed to create dataset {ds}: {e}")
                    return
            else:
                click.echo(f"Dataset {ds} does not exist. Use --force to create it.")
                return
    
    if datasets_exist or force:
        try:
            create_dataset_chain_views(client, qualified_datasets, tables_to_include)
            click.echo("Chain completed.")
        except Exception as e:
            click.echo(f"Error creating dataset chain: {e}")
    else:
        click.echo("Not all target datasets exist. Either create them or use --force.")


@dataset.command()
@click.argument("dataset")
@click.pass_context
def describe(ctx: Context, dataset: str) -> None:
    """Display detailed information about a specific dataset and its tables.

    Args:
        ctx: Click context object containing project and client information
        dataset: Dataset reference to describe. Can be specified as 'dataset' or 'project.dataset'

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
    describe_dataset(client, dataset, project)
