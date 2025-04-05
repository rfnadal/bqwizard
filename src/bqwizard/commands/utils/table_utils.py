import click
import csv


def validate_table_id(table: str) -> bool:
    """
    Validates that a table is passed in the dataset.table format

    Args:
        table (str): The table name in the format of dataset.table.

    Returns:
        bool: True if table is in the dataset.table format.

    """
    if len(table.split(".")) <= 1:
        raise click.BadArgumentUsage(
            "Please provide the table in the following format: dataset.table"
        )
    return True


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
