import click
import re


def _validate_bigquery_identifier(identifier: str, identifier_type: str = "identifier") -> bool:
    """
    Validates that an identifier follows BigQuery naming conventions.

    Args:
        identifier (str): The identifier to validate
        identifier_type (str): Type of identifier for error messages (project, dataset, table)

    Returns:
        bool: True if valid

    Raises:
        click.BadArgumentUsage: If identifier is invalid
    """
    if not identifier or not identifier.strip():
        raise click.BadArgumentUsage(f"{identifier_type.capitalize()} name cannot be empty")

    identifier = identifier.strip()

    # BigQuery identifier rules vary by type:
    if identifier_type == "project":
        # Project IDs are more flexible in practice - allow letters, numbers, hyphens, underscores
        # Be less strict to accommodate real-world project IDs
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$', identifier):
            raise click.BadArgumentUsage(
                f"Invalid {identifier_type} name '{identifier}'. "
                f"Project IDs must start and end with a letter or number, "
                f"and contain only letters, numbers, hyphens, and underscores."
            )
    else:
        # Dataset and table names: start with letter or underscore, contain letters, numbers, underscores
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise click.BadArgumentUsage(
                f"Invalid {identifier_type} name '{identifier}'. "
                f"Names must start with a letter or underscore and contain only letters, numbers, and underscores."
            )

    # Length limits
    length_limits = {"project": 30, "dataset": 1024, "table": 1024}
    max_length = length_limits.get(identifier_type, 1024)

    if len(identifier) > max_length:
        raise click.BadArgumentUsage(
            f"{identifier_type.capitalize()} name '{identifier}' exceeds maximum length of {max_length} characters"
        )

    return True


def get_table_id(project: str, table: str) -> str:
    """
    Helper function to handle table identifiers consistently.
    Determines if the table reference is fully qualified or not and formats it appropriately.

    Args:
        project (str): The project ID from the context
        table (str): The table reference which could be in the format:
                     - dataset.table
                     - project.dataset.table (fully qualified)
                     - `project.dataset.table` (with backticks)

    Returns:
        str: The properly formatted table_id

    Raises:
        click.BadArgumentUsage: If table format is invalid
    """
    if not table or not table.strip():
        raise click.BadArgumentUsage("Table reference cannot be empty")

    if not project or not project.strip():
        raise click.BadArgumentUsage("Project ID cannot be empty")

    # Strip whitespace and remove backticks if present
    table = table.strip().strip('`')
    project = project.strip()

    # Check for invalid empty parts (like dataset..table)
    if ".." in table:
        raise click.BadArgumentUsage(
            "Invalid table reference format. Consecutive dots are not allowed."
        )

    # Split on dots, but handle empty parts
    table_parts = [part.strip() for part in table.split(".") if part.strip()]

    if len(table_parts) == 3:
        # Validate that it's properly formatted and follows BigQuery naming rules
        proj, dataset, tbl = table_parts
        if not all([proj, dataset, tbl]):
            raise click.BadArgumentUsage(
                "Invalid table reference format. Please provide: dataset.table or project.dataset.table"
            )
        _validate_bigquery_identifier(proj, "project")
        _validate_bigquery_identifier(dataset, "dataset")
        _validate_bigquery_identifier(tbl, "table")
        return f"{proj}.{dataset}.{tbl}"
    elif len(table_parts) == 2:
        dataset, tbl = table_parts
        if not all([dataset, tbl]):
            raise click.BadArgumentUsage(
                "Invalid table reference format. Please provide: dataset.table or project.dataset.table"
            )
        _validate_bigquery_identifier(project, "project")
        _validate_bigquery_identifier(dataset, "dataset")
        _validate_bigquery_identifier(tbl, "table")
        return f"{project}.{dataset}.{tbl}"
    else:
        raise click.BadArgumentUsage(
            "Invalid table reference format. Please provide: dataset.table or project.dataset.table"
        )


def validate_table_id(table: str, validation_type: str = "short") -> bool:
    """
    Validates that a table is passed in the correct format.

    Args:
        table (str): The table reference to validate.
        validation_type (str): The expected format type:
                              - "short": Expects at least dataset.table format
                              - "full": Expects project.dataset.table format

    Returns:
        bool: True if table is in the correct format

    Raises:
        click.BadArgumentUsage: If table format is invalid
    """
    if not table or not table.strip():
        raise click.BadArgumentUsage("Table reference cannot be empty")

    # Strip whitespace and remove backticks if present
    table = table.strip().strip('`')

    # Split on dots, but handle empty parts
    table_parts = [part.strip() for part in table.split(".") if part.strip()]

    if validation_type == "short":
        if len(table_parts) < 2:
            raise click.BadArgumentUsage(
                "Please provide the table in the following format: dataset.table or project.dataset.table"
            )
        elif len(table_parts) == 2:
            dataset, tbl = table_parts
            if not all([dataset, tbl]):
                raise click.BadArgumentUsage(
                    "Invalid table reference format. Dataset and table names cannot be empty"
                )
            _validate_bigquery_identifier(dataset, "dataset")
            _validate_bigquery_identifier(tbl, "table")
        elif len(table_parts) == 3:
            proj, dataset, tbl = table_parts
            if not all([proj, dataset, tbl]):
                raise click.BadArgumentUsage(
                    "Invalid table reference format. Project, dataset, and table names cannot be empty"
                )
            _validate_bigquery_identifier(proj, "project")
            _validate_bigquery_identifier(dataset, "dataset")
            _validate_bigquery_identifier(tbl, "table")
        else:
            raise click.BadArgumentUsage(
                "Invalid table reference format. Too many dot-separated parts"
            )

    elif validation_type == "full":
        if len(table_parts) != 3:
            raise click.BadArgumentUsage(
                "Please provide the table in the following format: project.dataset.table"
            )
        proj, dataset, tbl = table_parts
        if not all([proj, dataset, tbl]):
            raise click.BadArgumentUsage(
                "Invalid table reference format. Project, dataset, and table names cannot be empty"
            )
        _validate_bigquery_identifier(proj, "project")
        _validate_bigquery_identifier(dataset, "dataset")
        _validate_bigquery_identifier(tbl, "table")

    else:
        raise click.BadArgumentUsage(
            f"Invalid validation type: {validation_type}. Use 'short' or 'full'"
        )

    return True