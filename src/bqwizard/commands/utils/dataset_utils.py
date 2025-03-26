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
    client = bigquery.Client()
    view_id = f"{target_dataset_ref}.{target_table}"
    create_view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS

    SELECT * FROM `{source_dataset_ref}`
    """
    create_table_query = client.query(create_view_query)
    click.echo(f"Table: {view_id} created successfully.")


def create_dataset(target_dataset_ref):
    client = bigquery.Client()
    client.create_dataset(target_dataset_ref)
    click.echo(f"Successfully created dataset: {target_dataset_ref}")