from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import click 
from tabulate import tabulate

def check_dataset_existence(client, dataset: str):
    """Check if a BigQuery dataset exists.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset ID to check
        
    Returns:
        bool: True if dataset exists, False otherwise
    """
    dataset_ref = client.dataset(dataset)
    return client.get_dataset(dataset_ref, retry=None) is not None
  
def create_view(client, source_dataset_ref: str, target_dataset_ref: str, target_table: str):
    """Create a BigQuery view in the target dataset that references a source table.
    
    Args:
        client: BigQuery client instance
        source_dataset_ref (str): Source dataset reference containing the original table
        target_dataset_ref (str): Target dataset reference where the view will be created
        target_table (str): Name of the table/view
        
    Returns:
        None: Prints success message upon completion
    """
    view_id = f"{target_dataset_ref}.{target_table}"
    create_view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS

    SELECT * FROM `{source_dataset_ref}.{target_table}`
    """
    create_table_query = client.query(create_view_query)
    create_table_query.result()
    click.echo(f"View: {view_id} created successfully. \n")


def create_dataset(client, target_dataset_ref):
    """Create a new BigQuery dataset.
    
    Args:
        client: BigQuery client instance
        target_dataset_ref: Dataset reference to create
        
    Returns:
        None: Prints success message upon completion
    """
    client.create_dataset(target_dataset_ref)
    click.echo(f"Successfully created dataset: {target_dataset_ref}")


def create_dataset_chain(client, datasets):
    """Create multiple datasets if they don't already exist.
    
    Args:
        client: BigQuery client instance
        datasets (list): List of dataset references to create
        
    Returns:
        None: Creates datasets if they don't exist
    """
    for dataset in datasets:
        if not check_dataset_existence(client, dataset):
            create_dataset(client, dataset)

def create_dataset_chain_views(client, datasets):
    """Create views in each subsequent dataset that reference tables from the previous dataset.
    
    Args:
        client: BigQuery client instance
        datasets (list): Ordered list of datasets. Views will be created in each dataset
                        referencing tables from the previous dataset in the list.
        
    Returns:
        None: Creates views and prints progress messages
    
    Example:
        If datasets = ['dataset1', 'dataset2', 'dataset3'] and dataset1 has tables ['table1', 'table2'],
        this will create views in dataset2 pointing to dataset1's tables, and views in dataset3 
        pointing to dataset2's views.
    """
    for index, dataset in enumerate(datasets):
            tables = [t.table_id for t in client.list_tables(dataset)]
            for table in tables:
                if index < (len(datasets) - 1):
                    click.echo(f"{dataset}.{table} --> {datasets[index + 1]}.{table}")
                    create_view(client, dataset, datasets[index + 1], table)

def describe_dataset(client, dataset, project):
    """Display detailed information about a BigQuery dataset and its tables.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset object to describe
        project: Project ID containing the dataset
        
    Returns:
        None: Prints formatted information about the dataset including:
            - Dataset ID
            - Description
            - Location
            - Labels
            - Table listing with details (ID, type, row count, last updated)
    """
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