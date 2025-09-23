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


def apply_dataset_transformation(dataset_name: str, remove_suffix: str = None, remove_prefix: str = None) -> str:
    """
    Apply transformation to dataset name by removing suffix or prefix.

    Args:
        dataset_name: Original dataset name
        remove_suffix: Suffix to remove (e.g., "_raw")
        remove_prefix: Prefix to remove (e.g., "raw_")

    Returns:
        str: Transformed dataset name

    Raises:
        click.BadArgumentUsage: If transformation results in empty name
    """
    transformed_name = dataset_name

    if remove_suffix and dataset_name.endswith(remove_suffix):
        transformed_name = dataset_name[:-len(remove_suffix)]

    if remove_prefix and dataset_name.startswith(remove_prefix):
        transformed_name = dataset_name[len(remove_prefix):]

    if not transformed_name:
        raise click.BadArgumentUsage(f"Transformation of '{dataset_name}' results in empty name")

    return transformed_name


def parse_layer_shorthand(layers_str: str, datasets_str: str, remove_suffix: str = None, remove_prefix: str = None) -> List[str]:
    """
    Parse layer shorthand syntax like --layers raw,clean,analytics --datasets customer_data

    Args:
        layers_str: Comma-separated layer names (e.g., "raw,clean,analytics")
        datasets_str: Comma-separated dataset names (e.g., "customer_data,user_data")
        remove_suffix: Optional suffix to remove from dataset names in target layers
        remove_prefix: Optional prefix to remove from dataset names in target layers

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
        for i, layer_name in enumerate(layer_names):
            # Apply transformation only to target layers (not the first layer)
            if i == 0:
                # First layer uses original name
                current_dataset_name = dataset_name
            else:
                # Subsequent layers use transformed name
                current_dataset_name = apply_dataset_transformation(dataset_name, remove_suffix, remove_prefix)

            layer_ref = f"{layer_name.lower()}.{current_dataset_name}"
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