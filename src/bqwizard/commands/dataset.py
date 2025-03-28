import click 
from google.cloud import bigquery
from .utils.dataset_utils import check_dataset_existance, create_view, create_dataset, create_dataset_chain, create_dataset_chain_views, describe_dataset
from tabulate import tabulate
import os
from typing import Tuple
from google.api_core.exceptions import NotFound


@click.group()
@click.pass_context
def dataset(ctx):
    """Manage Big Query Datasets"""
    ctx.ensure_object(dict)

@dataset.command()
@click.argument("dataset_name")
@click.pass_context
def tables(ctx, dataset_name: str) -> str:
    """List the tables in a dataset"""
    try:
        project = ctx.obj['PROJECT']
        client = ctx.obj['CLIENT']
        click.echo(f"Listing tables in dataset {dataset_name} of project {project}")
        dataset_ref = client.dataset(dataset_name, project=project)
        table_data = [(t.table_id, t.dataset_id, t.table_type) for t in client.list_tables(dataset_ref)]
        click.echo(tabulate(table_data, headers=["Table ID", "Dataset", "Type"], tablefmt="grid"))
    except Exception as e:
        click.echo(f"Error: {str(e)}")

@dataset.command()
@click.pass_context
def ls(ctx) -> str:
    """List datasets in a project"""
    try:
        project = ctx.obj['PROJECT']
        client = ctx.obj['CLIENT']
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
def describe_all(ctx):
  """List all datasets and it's corresponding tables."""
  client = ctx.obj["CLIENT"]
  project = ctx.obj["PROJECT"]
  datasets = list(client.list_datasets())
  for dataset_item in datasets:
    # Get full dataset information
    dataset = client.get_dataset(dataset_item.reference)
    describe_dataset(client, dataset, project)

@dataset.command()
@click.pass_context
@click.argument("dataset_name")
@click.option("--location", default="US")
def create(ctx, dataset_name: str, location: str):
    "Create a dataset"
    client = ctx.obj["CLIENT"]
    project = ctx.obj["PROJECT"]
    if project:
        try:
            dataset_ref = f"{project}.{dataset_name}"
            confirmation = click.confirm(f"Create dataset {dataset_ref} in location {location}?")
            if confirmation:
                click.echo(f"Creatiing dataset {dataset_ref} in location {location}")
                dataset = client.create_dataset(client, dataset_ref, timeout=30)
                click.echo(f"Successfully created dataset {dataset_ref} in location {location}")
        except Exception as e:
            click.echo(f"Unknow Exception Occured: {e}")
    else:
        click.echo("Please either pass a project id or set the GOOGLE_CLOUD_PROJECT environment variable.")


@dataset.command()
@click.pass_context
@click.argument("dataset_name")
def delete(ctx, dataset_name: str):
    "Delete a dataset"
    client = ctx.obj["CLIENT"]
    project = ctx.obj['PROJECT']
    try:
        if len(dataset_name.split('.')[0]) > 0:
            project = dataset_name.split('.')[0]
            dataset_name = dataset_name.split('.')[1]
        if project:
            dataset_ref = f"{project}.{dataset_name}"
            confirmation_1 = click.confirm(f"Delete dataset {dataset_ref}?")
            confirmation_2 = click.confirm(f"This is a distructive action are you sure?")
            if confirmation_1 and confirmation_2:
                client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
                click.echo(f"Successfully deleted the {dataset_ref} dataset.")
            else:
                click.echo("Deletion aborted")
        else:
            click.echo("Please either pass a project id or set the GOOGLE_CLOUD_PROJECT environment variable.")
    except Exception as e:
        click.echo(f"Unknown error occured: {e}")

@dataset.command()
@click.argument("source_project")
@click.argument("source_dataset")
@click.argument("target_project")
@click.argument("target_dataset")
@click.option("--force", help="Automatically create target datasets if they don't exist.", is_flag=True)
@click.pass_context
def expose(ctx, source_project: str, source_dataset: str, target_project: str, target_dataset: str, force: bool):
    "Expose all the tables or views in a datset as views in another dataset."
    try:
        client = ctx.obj['CLIENT']
        source_dataset_ref = f"{source_project}.{source_dataset}"
        target_dataset_ref = f"{target_project}.{target_dataset}"
        click.echo(f"Exposing dataset {source_dataset_ref} to {target_dataset_ref}")
        if check_dataset_existance(client, source_dataset_ref) and check_dataset_existance(client, target_dataset_ref):
            for tables in client.list_tables(source_dataset_ref):
                create_view(client, source_dataset_ref, target_dataset_ref, tables.table_id)
        elif check_dataset_existance(client, target_dataset_ref) == False and force:
            click.echo(f"Creating missing dataset {target_dataset_ref}.")
            create_dataset(client, target_dataset_ref)
            for tables in client.list_tables(source_dataset_ref):
                create_view(client, source_dataset_ref, target_dataset_ref, tables.table_id)
        else:
            click.echo("Error: Please make sure that source and target datasets exists.")
        click.echo("Done.")
    except Exception as e:
        click.echo(f"Unknown Exception Occured: {e}")

@dataset.command
@click.argument("datasets", nargs=-1, type=str)
@click.option("--force", help="Automatically create target datasets if they don't exist.", is_flag=True)
@click.pass_context
def chain(ctx, datasets: tuple, force: bool):
    """Create's a chain of datasets to expose data accross multiple datasets."""
    client = ctx.obj['CLIENT']
    datasets_exist = all([check_dataset_existance(client, dataset) for dataset in datasets]) 
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
def describe(ctx, dataset):
    """Describes the dataset and it's tables."""
    client = ctx.obj["CLIENT"]
    project = ctx.obj["PROJECT"]
    dataset = client.get_dataset(dataset)
    describe_dataset(client, dataset, project)
    
        
    