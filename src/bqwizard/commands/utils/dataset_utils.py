import click
from tabulate import tabulate


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
            project_id, dataset_id, _ = parts  # Ignore table part if present
        else:
            dataset_id = dataset
            project_id = client.project
        dataset_ref = client.dataset(dataset_id, project=project_id)
        return client.get_dataset(dataset_ref, retry=None) is not None
    except Exception:
        return False


def get_dataset_id(client, dataset_ref: str) -> str:
    """Helper function to handle dataset references consistently.
    
    Args:
        client: BigQuery client instance
        dataset_ref: Dataset reference which could be 'dataset' or 'project.dataset'
        
    Returns:
        str: The properly formatted dataset ID (project.dataset)
    """
    parts = dataset_ref.split('.')
    if len(parts) == 1:
        # Only dataset name provided, add project
        return f"{client.project}.{dataset_ref}"
    elif len(parts) == 2:
        # Already in project.dataset format
        return dataset_ref
    else:
        # Invalid format
        raise click.BadArgumentUsage(
            f"Invalid dataset reference format: {dataset_ref}. Expected 'dataset' or 'project.dataset'"
        )


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
    table_parts = table.split('.')
    
    if len(table_parts) == 3:
        # Already fully qualified as project.dataset.table
        return table
    elif len(table_parts) == 2:
        # Only dataset.table provided, prepend project
        return f"{project}.{table}"
    else:
        # Invalid format
        raise click.BadArgumentUsage(
            "Please provide the table in the following format: dataset.table or project.dataset.table"
        )


def create_view(
    client, source_table_ref: str, target_dataset_ref: str, target_table_name: str
):
    """Create a BigQuery view in the target dataset that references a source table.

    Args:
        client: BigQuery client instance
        source_table_ref (str): Source table reference (can be dataset.table or project.dataset.table)
        target_dataset_ref (str): Target dataset reference (can be dataset or project.dataset)
        target_table_name (str): Name of the view to be created (just the table name, not qualified)

    Returns:
        None: Prints success message upon completion
    """
    project = client.project
    
    # Use our local get_table_id function
    fq_source_table_id = get_table_id(project, source_table_ref)
    
    target_dataset_parts = target_dataset_ref.split('.')
    if len(target_dataset_parts) == 2:
        fq_target_view_id = f"{target_dataset_ref}.{target_table_name}"
    elif len(target_dataset_parts) == 1:
        fq_target_view_id = f"{project}.{target_dataset_ref}.{target_table_name}"
    else:
        raise click.BadParameter(f"Invalid target_dataset_ref format: {target_dataset_ref}")

    create_view_query = f"""
    CREATE OR REPLACE VIEW `{fq_target_view_id}` AS
    SELECT * FROM `{fq_source_table_id}`
    """
    create_table_query = client.query(create_view_query)
    create_table_query.result()
    click.echo(f"View: {fq_target_view_id} created successfully. \n")


def create_dataset(client, target_dataset_ref):
    """Create a new BigQuery dataset.

    Args:
        client: BigQuery client instance
        target_dataset_ref: Dataset reference to create (can be dataset or project.dataset)

    Returns:
        None: Prints success message upon completion
    """
    # Ensure dataset reference is properly formatted
    dataset_parts = target_dataset_ref.split('.')
    if len(dataset_parts) == 1:
        target_dataset_ref = f"{client.project}.{target_dataset_ref}"
    elif len(dataset_parts) > 2:
        raise click.BadParameter(f"Invalid target_dataset_ref format for creation: {target_dataset_ref}. Expected 'dataset' or 'project.dataset'.")

    client.create_dataset(target_dataset_ref)
    click.echo(f"Successfully created dataset: {target_dataset_ref}")


def create_dataset_chain(client, datasets):
    """Create multiple datasets if they don't already exist.

    Args:
        client: BigQuery client instance
        datasets (list): List of dataset references to create (can be dataset or project.dataset)

    Returns:
        None: Creates datasets if they don't exist
    """
    for dataset_ref in datasets:
        # Ensure dataset reference is properly formatted
        qualified_dataset = get_dataset_id(client, dataset_ref)
        
        if not check_dataset_existence(client, qualified_dataset):
            create_dataset(client, qualified_dataset)


def create_dataset_chain_views(client, datasets, tables_to_include=None):
    """Create views in each subsequent dataset that reference tables from the previous dataset.

    Args:
        client: BigQuery client instance
        datasets (list): Ordered list of dataset references. Views will be created in each dataset
                        referencing tables from the previous dataset in the list.
                        Dataset references can be 'dataset' or 'project.dataset'.
        tables_to_include (set, optional): Set of table names to include in the chain.
                                          If provided, only tables in this set will be included.

    Returns:
        None: Creates views and prints progress messages

    Example:
        If datasets = ['dataset1', 'projectA.dataset2', 'dataset3'] and dataset1 has tables ['table1', 'table2'],
        this will create views in projectA.dataset2 pointing to dataset1's tables (using client.project for dataset1),
        and views in dataset3 pointing to projectA.dataset2's views (using client.project for dataset3 if not specified).
    """
    project = client.project
    qualified_datasets = []
    for ds_ref in datasets:
        ds_parts = ds_ref.split('.')
        if len(ds_parts) == 1:
            qualified_datasets.append(f"{project}.{ds_ref}")
        elif len(ds_parts) == 2:
            qualified_datasets.append(ds_ref)
        else:
            click.echo(f"Skipping invalid dataset format in chain: {ds_ref}")
            continue

    for index, current_dataset_fq in enumerate(qualified_datasets):
        if index < (len(qualified_datasets) - 1):
            next_dataset_fq = qualified_datasets[index + 1]
            try:
                # Correctly handle the dataset reference
                dataset_parts = current_dataset_fq.split('.')
                if len(dataset_parts) == 2:
                    project_id, dataset_id = dataset_parts
                    # Create a proper dataset reference object
                    dataset_ref = client.dataset(dataset_id, project=project_id)
                    tables_list = client.list_tables(dataset_ref)
                else:
                    # This shouldn't happen as we qualified all datasets above
                    click.echo(f"Invalid dataset format: {current_dataset_fq}")
                    continue
                
                tables_in_current_dataset = [t.table_id for t in tables_list]
                
                for table_name in tables_in_current_dataset:
                    # Skip tables not in the include list if it's provided
                    if tables_to_include is not None and table_name not in tables_to_include:
                        click.echo(f"Skipping table {table_name} (not in tables CSV)")
                        continue
                        
                    source_table_ref_for_view = f"{current_dataset_fq}.{table_name}"
                    click.echo(f"{source_table_ref_for_view} --> {next_dataset_fq}.{table_name}")
                    create_view(client, source_table_ref_for_view, next_dataset_fq, table_name)
            except Exception as e:
                click.echo(f"Error processing dataset {current_dataset_fq}: {e}")


def describe_dataset(client, dataset, project):
    """Display detailed information about a BigQuery dataset and its tables.

    Args:
        client: BigQuery client instance
        dataset: Dataset object (google.cloud.bigquery.dataset.DatasetListItem or google.cloud.bigquery.dataset.Dataset)
                 OR a string dataset ID (e.g., 'mydataset' or 'myproject.mydataset')
        project: Project ID containing the dataset. This is primarily used if 'dataset' is a string ID without a project.
                 If 'dataset' is an object or a fully qualified string, this parameter might be redundant or used for fallback.

    Returns:
        None: Prints formatted information about the dataset including:
            - Dataset ID
            - Description
            - Location
            - Labels
            - Table listing with details (ID, type, row count, last modified)
    """
    dataset_id_to_fetch = None
    project_id_to_use = project

    if isinstance(dataset, str):
        dataset_parts = dataset.split('.')
        if len(dataset_parts) == 2:
            project_id_to_use, dataset_id_to_fetch = dataset_parts
        elif len(dataset_parts) == 1:
            dataset_id_to_fetch = dataset
        else:
            click.echo(f"Invalid dataset string format: {dataset}. Expected 'dataset' or 'project.dataset'.")
            return
    else:
        dataset_id_to_fetch = dataset.dataset_id
        if hasattr(dataset, 'project'):
             project_id_to_use = dataset.project
    
    if not dataset_id_to_fetch:
        click.echo("Could not determine dataset ID.")
        return

    try:
        # Create a proper dataset reference object and get the dataset
        dataset_ref = client.dataset(dataset_id_to_fetch, project=project_id_to_use)
        dataset_obj = client.get_dataset(dataset_ref)
    except Exception as e:
        click.echo(f"Could not fetch dataset {project_id_to_use}.{dataset_id_to_fetch}: {e}")
        return

    click.echo(f"Dataset: {dataset_obj.dataset_id}")
    click.echo(f"Description: {dataset_obj.description}")
    click.echo(f"Location: {dataset_obj.location}")
    click.echo("Labels:")
    labels = dataset_obj.labels
    if labels:
        for label, value in labels.items():
            click.echo(f"\t{label}: {value}")
    else:
        click.echo("\tDataset has no labels defined.")
    click.echo("\nTables:")
    
    table_data = []
    try:
        # Use the dataset reference object to list tables
        tables_in_dataset = list(client.list_tables(dataset_ref))
        for t in tables_in_dataset:
            try:
                # Create a proper table reference and get the table
                table_ref = client.get_table(
                    client.dataset(t.dataset_id, project=project_id_to_use).table(t.table_id)
                )
                table_data.append((
                    t.table_id,
                    t.dataset_id,
                    t.table_type,
                    table_ref.num_rows,
                    table_ref.modified,
                ))
            except Exception as e:
                click.echo(f"Error getting details for table {t.table_id}: {e}")
                table_data.append((
                    t.table_id,
                    t.dataset_id,
                    t.table_type,
                    "N/A",
                    "N/A",
                ))
    except Exception as e:
        click.echo(f"Error listing tables: {e}")
    
    click.echo(
        tabulate(
            table_data,
            headers=["Table ID", "Dataset", "Type", "Row Count", "Last Modified (UTC)"],
            tablefmt="grid",
        )
    )
