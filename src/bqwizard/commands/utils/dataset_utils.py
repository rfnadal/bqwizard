from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import click 

def check_dataset_existance(dataset: str):
    client = bigquery.Client()
    try: 
        exits = client.get_dataset(dataset)
        return True
    except NotFound:
        return False
  
def create_view(source_dataset_ref: str, target_dataset_ref: str, target_table: str):
    client = bigquery.Client(source_dataset_ref.split('.')[0])
    view_id = f"{target_dataset_ref}.{target_table}"
    create_view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS

    SELECT * FROM `{source_dataset_ref}.{target_table}`
    """
    create_table_query = client.query(create_view_query)
    create_table_query.result()
    click.echo(f"View: {view_id} created successfully. \n")


def create_dataset(target_dataset_ref):
    client = bigquery.Client()
    client.create_dataset(target_dataset_ref)
    click.echo(f"Successfully created dataset: {target_dataset_ref}")


def create_dataset_chain(datasets):
    for dataset in datasets:
        if not check_dataset_existance(dataset):
            create_dataset(dataset)

def create_dataset_chain_views(client, datasets):
    for index, dataset in enumerate(datasets):
            tables = [t.table_id for t in client.list_tables(dataset)]
            for table in tables:
                if index < (len(datasets) - 1):
                    click.echo(f"{dataset}.{table} --> {datasets[index + 1]}.{table}")
                    create_view(dataset, datasets[index + 1], table)