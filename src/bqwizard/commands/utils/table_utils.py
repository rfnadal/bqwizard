import click
import csv


def get_table_id(project: str, table: str) -> str:
    """
    Helper function to handle table identifiers consistently.
    Determines if the table reference is fully qualified or not and formats it appropriately.

    Args:
        project (str): The project ID from the context
        table (str): The table reference which could be in the format:
                     - table (single name, which is invalid)
                     - dataset.table
                     - project.dataset.table (fully qualified)

    Returns:
        str: The properly formatted table_id
    """
    table_parts = table.split(".")

    if len(table_parts) == 3:
        return table
    elif len(table_parts) == 2:
        return f"{project}.{table}"
    else:
        raise click.BadArgumentUsage(
            "Please provide the table in the following format: dataset.table or project.dataset.table"
        )


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
