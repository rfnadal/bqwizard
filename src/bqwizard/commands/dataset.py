import click 
from google.cloud import bigquery
from tabulate import tabulate
from typing import AnyStr
import os

@click.group()
@click.option("--project", envvar='GOOGLE_CLOUD_PROJECT')
@click.pass_context
def dataset(ctx, project: str):
    """Manage Big Query Datasets"""
    ctx.ensure_object(dict)
    ctx.obj['PROJECT'] = project
    client = bigquery.Client(project)
    ctx.obj['CLIENT'] = client

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
    project = ctx.obj['PROJECT']
    client = ctx.obj['CLIENT']
    datasets = list(client.list_datasets())
    if datasets:
        click.echo(f"Listing datasets in project {project}.")
        dataset_data = [[d.dataset_id] for d in datasets]
        click.echo(tabulate(dataset_data, headers=["Dataset ID"], tablefmt="grid"))
    else:
        print(f"{project} does not contain any datasets.")




