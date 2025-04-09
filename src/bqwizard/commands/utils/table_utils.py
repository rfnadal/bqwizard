import click
import csv
from .dataset_utils import check_dataset_existence, create_dataset


def validate_table_id(table: str, type: str = "short") -> bool:
    """
    Validates that a table is passed in the dataset.table format

    Args:
        table (str): The table name in the format of dataset.table.
        full (bool): Whether to perform full validation. Defaults to True.

    Returns:
        bool: True if table is in the dataset.table format.

    """
    if len(table.split(".")) <= 1 and type == "short":
        raise click.BadArgumentUsage(
            "Please provide the table in the following format: dataset.table"
        )
    elif len(table.split(".")) <= 2 and type == "full":
        raise click.BadArgumentUsage(
            "Please provide the table in the following format: project.dataset.table"
        )
    return True


def create_view(client, source_table, target_table, force):
    validate_table_id(source_table, "full")
    validate_table_id(target_table, "full")
    source_dataset_ok = check_dataset_existence(client, source_table)
    target_dataset_ok = check_dataset_existence(client, target_table)
    project, dataaset, table = target_table.split(".")
    target_dataset = f"{project}.{dataaset}"
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
