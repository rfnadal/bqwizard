from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import click 
from tabulate import tabulate

def check_dataset_existance(client, dataset: str):
    try: 
        exits = client.get_dataset(dataset)
        return True
    except NotFound:
        return False
  
def create_view(client, source_dataset_ref: str, target_dataset_ref: str, target_table: str):
    view_id = f"{target_dataset_ref}.{target_table}"
    create_view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS

    SELECT * FROM `{source_dataset_ref}.{target_table}`
    """
    create_table_query = client.query(create_view_query)
    create_table_query.result()
    click.echo(f"View: {view_id} created successfully. \n")


def create_dataset(client, target_dataset_ref):
    client.create_dataset(target_dataset_ref)
    click.echo(f"Successfully created dataset: {target_dataset_ref}")


def create_dataset_chain(client, datasets):
    for dataset in datasets:
        if not check_dataset_existance(client, dataset):
            create_dataset(client, dataset)

def create_dataset_chain_views(client, datasets):
    for index, dataset in enumerate(datasets):
            tables = [t.table_id for t in client.list_tables(dataset)]
            for table in tables:
                if index < (len(datasets) - 1):
                    click.echo(f"{dataset}.{table} --> {datasets[index + 1]}.{table}")
                    create_view(client, dataset, datasets[index + 1], table)

def describe_dataset(client, dataset, project):
    """Describe a dataset and its tables."""
    click.echo(f"Dataset: {dataset.dataset_id}")
    click.echo(f"Description: {dataset.description}")
    click.echo(f"Location: {dataset.location}")
    click.echo("Labels:")
    labels = dataset.labels
    if labels:
        for label, value in labels.items():
            click.echo(f"\t{label}: {value}")
    else:
        click.echo("\tDataset has no labels defined.")
    click.echo("\nTables:")
    table_data = [(t.table_id, t.dataset_id, t.table_type, client.get_table(f"{project}.{t.dataset_id}.{t.table_id}").num_rows, client.get_table(f"{project}.{t.dataset_id}.{t.table_id}").modified) for t in client.list_tables(dataset)]
    click.echo(tabulate(table_data, headers=["Table ID", "Dataset", "Type", "Row Count","Last Updated (UTC)"], tablefmt="grid"))