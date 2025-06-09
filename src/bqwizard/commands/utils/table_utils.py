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
    tables = set()
    
    # Remove comments and normalize whitespace
    sql_clean = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    sql_clean = re.sub(r'\s+', ' ', sql_clean)
    
    # Pattern for table references in FROM and JOIN clauses
    # Handles: FROM `project.dataset.table`, FROM dataset.table, JOIN `table` etc.
    from_join_pattern = r'(?:FROM|JOIN)\s+`?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){1,2})`?(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?'
    matches = re.findall(from_join_pattern, sql_clean, re.IGNORECASE)
    
    for match in matches:
        # Skip table aliases and functions
        if not match.lower().endswith(('(', 'unnest', 'generate_array', 'generate_date_array')):
            tables.add(match)
    
    # Also look for explicit backtick patterns anywhere in the query
    backtick_patterns = [
        r'`([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)`',  # project.dataset.table
        r'`([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)`',  # dataset.table
    ]
    
    for pattern in backtick_patterns:
        matches = re.findall(pattern, sql_clean)
        tables.update(matches)
    
    # Filter out obvious non-table references
    filtered_tables = set()
    for table in tables:
        # Must have at least one dot (dataset.table format)
        if '.' in table and not table.lower().startswith(('information_schema', 'temp')):
            filtered_tables.add(table)
    
    return filtered_tables


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


def get_view_dependencies(client, project: str, view_reference: str) -> Set[str]:
    """
    Get the dependencies of a specific view by parsing its SQL definition.
    
    Args:
        client: BigQuery client
        project (str): Project ID
        view_reference (str): The view reference to get dependencies for
        
    Returns:
        Set[str]: Set of table/view references that this view depends on
    """
    try:
        # Get the view definition
        table_obj = client.get_table(view_reference)
        
        if table_obj.table_type != "VIEW":
            return set()
        
        view_sql = table_obj.view_query
        return extract_table_references(view_sql)
        
    except Exception as e:
        click.echo(f"Error getting view dependencies for {view_reference}: {str(e)}")
        return set()


def build_dependency_chain(client, project: str, starting_view: str, max_depth: int = 10) -> List[str]:
    """
    Build a dependency chain by working backwards from a view to find all its dependencies.
    
    Args:
        client: BigQuery client
        project (str): Project ID
        starting_view (str): The starting view reference
        max_depth (int): Maximum depth to traverse
        
    Returns:
        List[str]: Ordered list of views to refresh (dependencies first, then dependents)
    """
    visited = set()
    all_views = set()
    
    def collect_dependencies(view_ref: str, depth: int = 0):
        if depth >= max_depth or view_ref in visited:
            return
        
        visited.add(view_ref)
        
        # Get dependencies of this view
        dependencies = get_view_dependencies(client, project, view_ref)
        
        for dep_ref in dependencies:
            # Normalize the dependency reference
            if len(dep_ref.split('.')) == 2:
                full_dep_ref = f"{project}.{dep_ref}"
            else:
                full_dep_ref = dep_ref
            
            # Check if this dependency is also a view
            try:
                dep_obj = client.get_table(full_dep_ref)
                if dep_obj.table_type == "VIEW":
                    all_views.add(full_dep_ref)
                    # Recursively collect dependencies of this view
                    collect_dependencies(full_dep_ref, depth + 1)
            except Exception:
                # Dependency might be a table or doesn't exist, skip
                continue
    
    # Start collection from the given view
    try:
        table_obj = client.get_table(starting_view)
        if table_obj.table_type == "VIEW":
            all_views.add(starting_view)
            collect_dependencies(starting_view)
        else:
            click.echo(f"Warning: {starting_view} is not a view")
            return []
    except Exception as e:
        click.echo(f"Error: Could not access {starting_view}: {str(e)}")
        return []
    
    # Now we need to order the views correctly - dependencies first
    refresh_order = []
    remaining_views = all_views.copy()
    
    # Build the correct refresh order using topological sort approach
    while remaining_views:
        # Find views that have no unprocessed dependencies
        ready_views = []
        
        for view_ref in remaining_views:
            view_deps = get_view_dependencies(client, project, view_ref)
            # Check if all dependencies are either not views or already processed
            deps_ready = True
            for dep in view_deps:
                if len(dep.split('.')) == 2:
                    full_dep = f"{project}.{dep}"
                else:
                    full_dep = dep
                
                if full_dep in remaining_views:
                    deps_ready = False
                    break
            
            if deps_ready:
                ready_views.append(view_ref)
        
        if not ready_views:
            # Circular dependency or other issue
            click.echo("Warning: Possible circular dependency detected")
            refresh_order.extend(list(remaining_views))
            break
        
        # Add ready views to refresh order and remove from remaining
        refresh_order.extend(ready_views)
        for view in ready_views:
            remaining_views.remove(view)
    
    return refresh_order


def refresh_view_recursive(client, project: str, view_reference: str, dry_run: bool = False) -> Dict:
    """
    Recursively refresh a view and all its view dependencies.
    
    Args:
        client: BigQuery client
        project (str): Project ID
        view_reference (str): The view to start from
        dry_run (bool): If True, only show what would be refreshed
        
    Returns:
        Dict: Results of the refresh operation
    """
    results = {
        "starting_view": view_reference,
        "refresh_order": [],
        "refreshed": [],
        "failed": [],
        "dry_run": dry_run
    }
    
    # Build dependency chain working backwards from the view
    refresh_order = build_dependency_chain(client, project, view_reference)
    results["refresh_order"] = refresh_order
    
    if not refresh_order:
        click.echo(f"No view dependencies found for {view_reference}")
        return results
    
    click.echo(f"Found {len(refresh_order)} views in dependency chain:")
    for i, view_ref in enumerate(refresh_order, 1):
        click.echo(f"  {i}. {view_ref}")
    
    if dry_run:
        click.echo("\nDry run mode - no views will be refreshed")
        return results
    
    # Refresh views in dependency order (dependencies first)
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
