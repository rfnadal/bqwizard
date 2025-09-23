import click
import csv
from typing import Dict, List
from google.api_core.exceptions import NotFound
from .dataset_utils import check_dataset_existence
from .dataset_utils import create_dataset
from .reference_utils import get_table_id, validate_table_id


def create_view(client, source_table, target_table, force):
    """
    Creates a view from a source table to a target table.

    Args:
        client: BigQuery client
        source_table (str): Fully qualified source table ID
        target_table (str): Fully qualified target table ID
        force (bool): Whether to automatically create datasets if they don't exist
    """
    # Validate both source and target table references
    validate_table_id(source_table, "full")
    validate_table_id(target_table, "full")

    source_dataset_ok = check_dataset_existence(client, source_table)
    target_dataset_ok = check_dataset_existence(client, target_table)

    # Safely split the target table reference
    target_parts = [part.strip() for part in target_table.split(".") if part.strip()]
    if len(target_parts) != 3:
        raise click.BadArgumentUsage(f"Invalid target table format: {target_table}")

    project, dataset, table = target_parts
    target_dataset = f"{project}.{dataset}"

    if source_dataset_ok and target_dataset_ok:
        view_query = f"""
            CREATE VIEW `{target_table}` AS SELECT * FROM `{source_table}`
            """
        view_query_job = client.query(view_query)
        view_query_job.result()
        click.echo(f"View: {target_table} created successfully. \n")
    elif (source_dataset_ok or target_dataset_ok) and force:
        create_dataset(client, target_dataset)
        view_query = f"""
            CREATE VIEW `{target_table}` AS SELECT * FROM `{source_table}`
            """
        view_query_job = client.query(view_query)
        view_query_job.result()
        click.echo(f"View: {target_table} created successfully. \n")
    else:
        click.echo("Target dataset does not exist. Either create them or use --force.")


def write_to_csv(data, dest: str) -> None:
    """
    Write's the result of a BQ Query to a csv

    Args:
        data: The data the user wants to write to a a csv.
        dest (str): The filename/path for the CSV file to write.
    Returns:
        None
    """
    with open(dest, "w") as f:
        headers = [header.name for header in data.schema]
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)


def get_views_in_dataset(client, project: str, dataset: str) -> List[Dict]:
    """
    Get all views in a specific dataset.

    Args:
        client: BigQuery client
        project (str): Project ID
        dataset (str): Dataset ID

    Returns:
        List[Dict]: List of view information dictionaries
    """
    query = f"""
    SELECT 
        table_catalog as project_id,
        table_schema as dataset_id,
        table_name as view_name,
        view_definition
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
    ORDER BY table_name
    """

    try:
        results = client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        click.echo(f"Error querying views in dataset {dataset}: {str(e)}")
        return []
