import click
import csv
import re
from typing import Dict, List, Set, Tuple
from google.api_core.exceptions import NotFound


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
    dataset_parts = target_dataset_ref.split(".")
    if len(dataset_parts) == 1:
        target_dataset_ref = f"{client.project}.{target_dataset_ref}"
    elif len(dataset_parts) > 2:
        raise click.BadParameter(
            f"Invalid target_dataset_ref format for creation: {target_dataset_ref}. Expected 'dataset' or 'project.dataset'."
        )

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
    table_parts = table.split(".")

    if len(table_parts) == 3:
        return table
    elif len(table_parts) == 2:
        return f"{project}.{table}"
    else:
        validate_table_id(table)
        return f"{project}.{table}"


def extract_table_references(sql_query: str) -> Set[str]:
    """
    Extract table references from a SQL query using regex patterns.
    
    Args:
        sql_query (str): The SQL query to parse
        
    Returns:
        Set[str]: Set of table references found in the query
    """
    patterns = [
        r'`([^`]+\.[^`]+\.[^`]+)`',  # project.dataset.table
        r'`([^`]+\.[^`]+)`',         # dataset.table
    ]
    
    tables = set()
    
    # Look for FROM and JOIN clauses with table references
    from_join_pattern = r'(?:FROM|JOIN)\s+([`]?[a-zA-Z_][a-zA-Z0-9_.]*[`]?)'
    matches = re.findall(from_join_pattern, sql_query, re.IGNORECASE)
    
    for match in matches:
        clean_match = match.strip('`')
        if '.' in clean_match:
            tables.add(clean_match)
    
    # Also check for explicit patterns
    for pattern in patterns:
        matches = re.findall(pattern, sql_query)
        tables.update(matches)
    
    return tables


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


def find_dependent_views(client, project: str, table_reference: str) -> List[Dict]:
    """
    Find all views that depend on a specific table or view.
    
    Args:
        client: BigQuery client
        project (str): Project ID
        table_reference (str): The table/view reference to find dependencies for
        
    Returns:
        List[Dict]: List of dependent view information
    """
    # Parse table reference to get dataset and table name
    parts = table_reference.split('.')
    if len(parts) == 2:
        dataset, table = parts
    elif len(parts) == 3:
        _, dataset, table = parts
    else:
        click.echo(f"Invalid table reference format: {table_reference}")
        return []
    
    # Search for views that reference this table
    escaped_dataset = re.escape(dataset)
    escaped_table = re.escape(table)
    query = f"""
    SELECT 
        table_catalog as project_id,
        table_schema as dataset_id,
        table_name as view_name,
        view_definition
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
    WHERE REGEXP_CONTAINS(view_definition, r'(?i){escaped_dataset}\.{escaped_table}')
    ORDER BY table_name
    """
    
    try:
        results = client.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        click.echo(f"Error finding dependent views: {str(e)}")
        return []


def build_dependency_chain(client, project: str, starting_table: str, max_depth: int = 10) -> List[str]:
    """
    Build a dependency chain for views starting from a base table.
    
    Args:
        client: BigQuery client
        project (str): Project ID
        starting_table (str): The starting table/view reference
        max_depth (int): Maximum depth to traverse
        
    Returns:
        List[str]: Ordered list of views to refresh (bottom-up)
    """
    visited = set()
    refresh_order = []
    
    def traverse_dependencies(table_ref: str, depth: int = 0):
        if depth >= max_depth or table_ref in visited:
            return
        
        visited.add(table_ref)
        
        # Find views that depend on this table/view
        dependent_views = find_dependent_views(client, project, table_ref)
        
        for view_info in dependent_views:
            view_ref = f"{view_info['dataset_id']}.{view_info['view_name']}"
            full_view_ref = f"{project}.{view_ref}"
            
            if full_view_ref not in visited:
                # Recursively find dependencies of this view
                traverse_dependencies(full_view_ref, depth + 1)
                
                # Add to refresh order if not already present
                if full_view_ref not in refresh_order:
                    refresh_order.append(full_view_ref)
    
    traverse_dependencies(starting_table)
    return refresh_order


def refresh_view_recursive(client, project: str, table_reference: str, dry_run: bool = False) -> Dict:
    """
    Recursively refresh views that depend on a table or view.
    
    Args:
        client: BigQuery client
        project (str): Project ID
        table_reference (str): The table/view to start from
        dry_run (bool): If True, only show what would be refreshed
        
    Returns:
        Dict: Results of the refresh operation
    """
    results = {
        "starting_table": table_reference,
        "refresh_order": [],
        "refreshed": [],
        "failed": [],
        "dry_run": dry_run
    }
    
    # Build dependency chain
    refresh_order = build_dependency_chain(client, project, table_reference)
    results["refresh_order"] = refresh_order
    
    if not refresh_order:
        click.echo(f"No dependent views found for {table_reference}")
        return results
    
    click.echo(f"Found {len(refresh_order)} views to refresh:")
    for i, view_ref in enumerate(refresh_order, 1):
        click.echo(f"  {i}. {view_ref}")
    
    if dry_run:
        click.echo("\nDry run mode - no views will be refreshed")
        return results
    
    # Refresh views in order
    for view_ref in refresh_order:
        try:
            # Get the view definition
            table_obj = client.get_table(view_ref)
            
            if table_obj.table_type == "VIEW":
                view_sql = table_obj.view_query
                refresh_query = f"CREATE OR REPLACE VIEW `{view_ref}` AS {view_sql}"
                
                query_job = client.query(refresh_query)
                query_job.result()
                
                results["refreshed"].append(view_ref)
                click.echo(f"✓ Refreshed view: {view_ref}")
            else:
                click.echo(f"⚠ Skipping {view_ref} - not a view")
                
        except Exception as e:
            results["failed"].append({"view": view_ref, "error": str(e)})
            click.echo(f"✗ Failed to refresh {view_ref}: {str(e)}")
    
    return results
