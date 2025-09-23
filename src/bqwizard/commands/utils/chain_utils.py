"""
Utilities for dataset and table chaining operations with enhanced error handling.
"""
import click
from typing import List, Dict, Set, Optional
from google.api_core.exceptions import NotFound, BadRequest
from .dataset_utils import create_view


class ChainOperationTracker:
    """Tracks chain operations for monitoring and reporting."""

    def __init__(self, client, dry_run: bool = False):
        self.client = client
        self.dry_run = dry_run
        self.operation_log: List[Dict] = []

    def create_view_with_tracking(self, source_table: str, target_dataset: str, target_table: str) -> bool:
        """Create a view and track the operation.

        Args:
            source_table: Fully qualified source table ID
            target_dataset: Target dataset ID
            target_table: Target table name

        Returns:
            bool: True if successful, False otherwise
        """
        target_view_id = f"{target_dataset}.{target_table}"

        if self.dry_run:
            self.operation_log.append({
                "operation": "create_view",
                "target": target_view_id,
                "source": source_table,
                "status": "would_create"
            })
            return True

        try:
            create_view(self.client, source_table, target_dataset, target_table)
            self.operation_log.append({
                "operation": "create_view",
                "target": target_view_id,
                "source": source_table,
                "status": "success"
            })
            return True

        except Exception as e:
            self.operation_log.append({
                "operation": "create_view",
                "target": target_view_id,
                "source": source_table,
                "status": "failed",
                "error": str(e)
            })
            click.echo(f"Failed to create view {target_view_id}: {e}", err=True)
            return False

    def create_dataset_with_tracking(self, dataset_id: str) -> bool:
        """Create a dataset and track the operation.

        Args:
            dataset_id: Fully qualified dataset ID

        Returns:
            bool: True if successful, False otherwise
        """
        if self.dry_run:
            self.operation_log.append({
                "operation": "create_dataset",
                "target": dataset_id,
                "status": "would_create"
            })
            return True

        try:
            from .dataset_utils import create_dataset
            create_dataset(self.client, dataset_id)
            self.operation_log.append({
                "operation": "create_dataset",
                "target": dataset_id,
                "status": "success"
            })
            return True

        except Exception as e:
            self.operation_log.append({
                "operation": "create_dataset",
                "target": dataset_id,
                "status": "failed",
                "error": str(e)
            })
            click.echo(f"Failed to create dataset {dataset_id}: {e}", err=True)
            return False


    def print_operation_summary(self) -> None:
        """Print a summary of all operations performed."""
        if not self.operation_log:
            return

        click.echo("\nOperation Summary:")
        click.echo("-" * 30)

        success_count = sum(1 for op in self.operation_log if op["status"] == "success")
        failed_count = sum(1 for op in self.operation_log if op["status"] == "failed")
        would_create_count = sum(1 for op in self.operation_log if op["status"] == "would_create")

        if self.dry_run:
            click.echo(f"Views that would be created: {would_create_count}")
        else:
            click.echo(f"Successfully created: {success_count}")
            if failed_count > 0:
                click.echo(f"Failed operations: {failed_count}")

        # Show failed operations details
        failed_ops = [op for op in self.operation_log if op["status"] == "failed"]
        if failed_ops:
            click.echo("\nFailed Operations:")
            for op in failed_ops:
                click.echo(f"  - {op['operation']}: {op['target']} - {op.get('error', 'Unknown error')}")


def create_dataset_chain_with_tracking(
    client,
    datasets: List[str],
    tables_to_include: Optional[Set[str]] = None,
    dry_run: bool = False
) -> bool:
    """
    Create dataset chain with enhanced error handling and operation tracking.

    Args:
        client: BigQuery client instance
        datasets: List of dataset references in chain order
        tables_to_include: Set of table names to include in the chain
        dry_run: If True, only shows what would be created

    Returns:
        bool: True if all operations successful, False otherwise
    """
    tracker = ChainOperationTracker(client, dry_run)
    project = client.project

    # Qualify all dataset references
    qualified_datasets = []
    for ds_ref in datasets:
        ds_parts = ds_ref.split(".")
        if len(ds_parts) == 1:
            qualified_datasets.append(f"{project}.{ds_ref}")
        elif len(ds_parts) == 2:
            qualified_datasets.append(ds_ref)
        else:
            click.echo(f"Invalid dataset format: {ds_ref}", err=True)
            return False

    success = True
    total_operations = 0
    completed_operations = 0

    try:
        # Process each dataset pair in the chain
        for index, current_dataset_fq in enumerate(qualified_datasets):
            if index < (len(qualified_datasets) - 1):
                next_dataset_fq = qualified_datasets[index + 1]

                try:
                    dataset_parts = current_dataset_fq.split(".")
                    if len(dataset_parts) == 2:
                        project_id, dataset_id = dataset_parts
                        dataset_ref = client.dataset(dataset_id, project=project_id)
                        tables_list = client.list_tables(dataset_ref)
                    else:
                        click.echo(f"Invalid dataset format: {current_dataset_fq}", err=True)
                        continue

                    tables_in_current_dataset = [t.table_id for t in tables_list]

                    # Filter tables if specified
                    if tables_to_include:
                        tables_in_current_dataset = [
                            t for t in tables_in_current_dataset
                            if t in tables_to_include
                        ]

                    total_operations += len(tables_in_current_dataset)

                    # Create views for each table
                    for table_name in tables_in_current_dataset:
                        source_table_ref = f"{current_dataset_fq}.{table_name}"

                        if not tracker.create_view_with_tracking(
                            source_table_ref, next_dataset_fq, table_name
                        ):
                            success = False
                            break
                        completed_operations += 1

                    if not success:
                        break

                except Exception as e:
                    click.echo(f"Error processing dataset {current_dataset_fq}: {e}", err=True)
                    success = False
                    break

    except Exception as e:
        click.echo(f"Unexpected error during chain creation: {e}", err=True)
        success = False

    # Handle results
    tracker.print_operation_summary()

    return success