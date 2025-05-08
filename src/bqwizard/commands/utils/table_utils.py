import click
import csv


def check_dataset_existence(client, dataset: str):
    """Check if a BigQuery dataset exists.

    Args:
        client: BigQuery client instance
        dataset: Dataset reference to check (can include project ID)

    Returns:
        bool: True if dataset exists, False otherwise
    """
    try:
        parts = dataset.split(".")
        if len(parts) == 2:
            project_id, dataset_id = parts
        elif len(parts) == 3:
            project_id, dataset_id, _ = parts  
        else:
            dataset_id = dataset
            project_id = client.project
        dataset_ref = client.dataset(dataset_id, project=project_id)
        return client.get_dataset(dataset_ref, retry=None) is not None
    except Exception:
        return False


def create_dataset(client, target_dataset_ref):
    """Create a new BigQuery dataset.

    Args:
        client: BigQuery client instance
        target_dataset_ref: Dataset reference to create (can be dataset or project.dataset)

    Returns:
        None: Prints success message upon completion
    """
    dataset_parts = target_dataset_ref.split('.')
    if len(dataset_parts) == 1:
        target_dataset_ref = f"{client.project}.{target_dataset_ref}"
    elif len(dataset_parts) > 2:
        raise click.BadParameter(f"Invalid target_dataset_ref format for creation: {target_dataset_ref}. Expected 'dataset' or 'project.dataset'.")

    client.create_dataset(target_dataset_ref)
    click.echo(f"Successfully created dataset: {target_dataset_ref}")


def validate_table_id(table: str, type: str = "short") -> bool:
    """
    Validates that a table is passed in the correct format (dataset.table or project.dataset.table)

    Args:
        table (str): The table reference to validate.
        type (str): The expected format type:
                    - "short": Expects at least dataset.table format
                    - "full": Expects project.dataset.table format

    Returns:
        bool: True if table is in the correct format, raises an exception otherwise.
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
    """
    Creates a view from a source table to a target table.
    
    Args:
        client: BigQuery client
        source_table (str): Fully qualified source table ID
        target_table (str): Fully qualified target table ID
        force (bool): Whether to automatically create datasets if they don't exist
    """
    source_dataset_ok = check_dataset_existence(client, source_table)
    target_dataset_ok = check_dataset_existence(client, target_table)
    
    project, dataset, table = target_table.split(".")
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


def get_table_id(project: str, table: str) -> str:
    """
    Helper function to handle table identifiers consistently.
    Determines if the table reference is fully qualified or not and formats it appropriately.
    
    Args:
        project (str): The project ID from the context
        table (str): The table reference which could be in the format:
                     - table (single name, invalid but caught by validate_table_id)
                     - dataset.table
                     - project.dataset.table (fully qualified)
    
    Returns:
        str: The properly formatted table_id
    """
    table_parts = table.split('.')
    
    if len(table_parts) == 3:
        return table
    elif len(table_parts) == 2:
        return f"{project}.{table}"
    else:
        validate_table_id(table)
        return f"{project}.{table}"
