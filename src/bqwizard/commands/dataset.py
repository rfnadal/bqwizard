import click 
from google.cloud import bigquery
from tabulate import tabulate
from typing import AnyStr

@click.group()
def dataset():
    """Manage Big Query Datasets"""
    pass

@dataset.command()
@click.argument("project")
@click.argument("dataset_name")
def tables(project: str, dataset_name: str) -> str:
    """List the tables in a dataset"""
    try:
        client = bigquery.Client()
        click.echo(f"Listing tables in dataset {dataset_name} of project {project}")
        dataset_ref = client.dataset(dataset_name, project=project)
        table_data = [(t.table_id, t.dataset_id, t.table_type) for t in client.list_tables(dataset_ref)]
        click.echo(tabulate(table_data, headers=["Table ID", "Dataset", "Type"], tablefmt="grid"))
    except Exception as e:
        click.echo(f"Error: {str(e)}")

@dataset.command()
@click.argument("project")
def list(project: str) -> str:
    """List datasets in a project"""
    client = bigquery.Client()
    datasets = list(client.list_datasets())
    if datasets:
        click.echo(f"Listing datasets in project {project}.")
        dataset_data = [[d.dataset_id] for d in datasets]
        click.echo(tabulate(dataset_data, headers=["Dataset ID"], tablefmt="grid"))
    else:
        print(f"{project} does not contain any datasets.")


def create():
    "Creates a dataset in Big Query"
    pass
    

