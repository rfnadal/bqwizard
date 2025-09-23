"""
Utilities for resolving layer references in dataset and table names.
"""
import click
from typing import List, Optional, Tuple
from .config_utils import get_layer_project, get_all_layers


def resolve_layer_reference(reference: str) -> str:
    """
    Resolve a layer reference to a fully qualified project.dataset format.

    Args:
        reference: Dataset reference that may contain layer names (e.g., "raw.customer_data")

    Returns:
        str: Fully qualified dataset reference (e.g., "project1.customer_data")

    Raises:
        click.BadArgumentUsage: If layer is not configured or reference is invalid
    """
    if not reference or not reference.strip():
        raise click.BadArgumentUsage("Reference cannot be empty")

    reference = reference.strip()

    # Split the reference
    parts = reference.split(".")

    if len(parts) == 1:
        # Just a dataset name, no layer or project
        return reference
    elif len(parts) == 2:
        # Could be layer.dataset or project.dataset
        layer_or_project, dataset = parts

        # Check if the first part is a configured layer
        project_id = get_layer_project(layer_or_project.lower())
        if project_id:
            return f"{project_id}.{dataset}"
        else:
            # Assume it's already project.dataset format
            return reference
    elif len(parts) == 3:
        # Already fully qualified project.dataset.table or project.dataset format
        return reference
    else:
        raise click.BadArgumentUsage(f"Invalid reference format: {reference}")


def resolve_layer_references(references: List[str]) -> List[str]:
    """
    Resolve multiple layer references.

    Args:
        references: List of dataset references that may contain layer names

    Returns:
        List[str]: List of fully qualified dataset references
    """
    resolved = []
    for ref in references:
        try:
            resolved.append(resolve_layer_reference(ref))
        except click.BadArgumentUsage as e:
            raise click.BadArgumentUsage(f"Error resolving '{ref}': {str(e)}")

    return resolved


def parse_layer_shorthand(layers_str: str, datasets_str: str) -> List[str]:
    """
    Parse layer shorthand syntax like --layers raw,clean,analytics --datasets customer_data

    Args:
        layers_str: Comma-separated layer names (e.g., "raw,clean,analytics")
        datasets_str: Comma-separated dataset names (e.g., "customer_data,user_data")

    Returns:
        List[str]: Fully qualified dataset references

    Raises:
        click.BadArgumentUsage: If layers are not configured or format is invalid
    """
    if not layers_str or not datasets_str:
        raise click.BadArgumentUsage("Both layers and datasets must be provided")

    layer_names = [l.strip() for l in layers_str.split(",") if l.strip()]
    dataset_names = [d.strip() for d in datasets_str.split(",") if d.strip()]

    if not layer_names:
        raise click.BadArgumentUsage("No valid layer names provided")

    if not dataset_names:
        raise click.BadArgumentUsage("No valid dataset names provided")

    # Validate all layers are configured
    for layer_name in layer_names:
        if not get_layer_project(layer_name.lower()):
            raise click.BadArgumentUsage(f"Layer '{layer_name}' not configured. Use 'bqwizard config set-layer {layer_name} <project>' first.")

    # Generate all combinations
    result = []
    for dataset_name in dataset_names:
        for layer_name in layer_names:
            layer_ref = f"{layer_name.lower()}.{dataset_name}"
            resolved_ref = resolve_layer_reference(layer_ref)
            result.append(resolved_ref)

    return result


def validate_layer_syntax(reference: str) -> Tuple[bool, Optional[str]]:
    """
    Validate if a reference uses valid layer syntax.

    Args:
        reference: Dataset reference to validate

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    try:
        resolve_layer_reference(reference)
        return True, None
    except click.BadArgumentUsage as e:
        return False, str(e)


def get_layer_suggestions() -> List[str]:
    """
    Get suggestions for available layers.

    Returns:
        List[str]: List of configured layer names
    """
    layers = get_all_layers()
    return sorted(layers.keys())


def format_layer_help() -> str:
    """
    Generate help text showing available layers.

    Returns:
        str: Formatted help text
    """
    layers = get_all_layers()

    if not layers:
        return "No layers configured. Use 'bqwizard config set-layer <layer> <project>' to add layers."

    help_text = "Available layers:\n"
    for layer_name, project_id in sorted(layers.items()):
        help_text += f"  {layer_name} -> {project_id}\n"

    help_text += "\nUsage examples:\n"
    help_text += "  dataset reference: raw.customer_data -> resolved to appropriate project\n"
    help_text += "  chain shorthand: --layers raw,clean,analytics --datasets customer_data\n"

    return help_text