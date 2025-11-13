import click
from click import Context
from .utils.config_utils import (
    get_user_config_dir,
    load_config,
    update_config,
    set_layer,
    get_layer_project,
    get_all_layers,
    remove_layer
)
from tabulate import tabulate


@click.group()
@click.pass_context
def config(ctx: Context) -> None:
    """Manage BQWIZARD configuration settings.

    Args:
        ctx: Click context object for managing shared state between commands
    """
    pass


@config.command()
@click.argument("project")
def set_project(project: str) -> None:
    """Set the GCP project for the current session.

    Args:
        project (str): The GCP project ID to set as active

    Returns:
        None: Prints confirmation message upon successful update

    Raises:
        Exception: If there's an error updating the configuration
    """
    try:
        config_file_dir = get_user_config_dir()
        settings = load_config(config_file_dir)
        click.echo(f"Setting GCP Project to {project}...")
        updated_settings = settings.model_copy(update={"project": f"{project}"})
        update_config(config_file_dir, updated_settings)
        click.echo("Done.")
    except Exception as e:
        click.echo(f"Error updating project configuration: {str(e)}")


@config.command()
def show() -> None:
    """Display the current configuration settings.

    Returns:
        None: Prints formatted table of current configuration settings

    Raises:
        Exception: If there's an error loading or displaying the configuration
    """
    try:
        config_file_dir = get_user_config_dir()
        settings = load_config(config_file_dir)
        click.echo("\nCurrent Configuration:")
        click.echo("-" * 30)
        for key, value in settings.model_dump().items():
            click.echo(f"{key}: {value}")
        click.echo("-" * 30)
    except Exception as e:
        click.echo(f"Error displaying configuration: {str(e)}")


@config.command("set-layer")
@click.argument("layer_name")
@click.argument("project_id")
def set_layer_command(layer_name: str, project_id: str) -> None:
    """Set a layer to project mapping.

    Args:
        layer_name: Name of the layer (e.g., 'raw', 'clean', 'analytics')
        project_id: Project ID for this layer

    Examples:
        bqwizard config set-layer raw project1-raw
        bqwizard config set-layer clean project1-clean
        bqwizard config set-layer analytics project1-analytics
    """
    try:
        # Validate project_id format (basic validation)
        if not project_id or not project_id.strip():
            click.echo("Project ID cannot be empty", err=True)
            return

        # Validate layer name format
        if not layer_name or not layer_name.strip() or ' ' in layer_name:
            click.echo("Layer name must be a single word without spaces", err=True)
            return

        layer_name = layer_name.strip().lower()
        project_id = project_id.strip()

        set_layer(layer_name, project_id)
        click.echo(f"Layer '{layer_name}' set to project '{project_id}'")
    except Exception as e:
        click.echo(f"Error setting layer: {str(e)}", err=True)


@config.command("list-layers")
def list_layers() -> None:
    """List all configured layers.

    Shows all layer to project mappings currently configured.
    """
    try:
        layers = get_all_layers()

        if not layers:
            click.echo("No layers configured.")
            click.echo("\nUse 'bqwizard config set-layer <layer> <project>' to add layers.")
            return

        click.echo("\nConfigured Layers:")
        click.echo("-" * 40)

        # Format as table
        table_data = [[layer, project] for layer, project in sorted(layers.items())]
        click.echo(tabulate(table_data, headers=["Layer", "Project ID"], tablefmt="simple"))

        click.echo(f"\nTotal: {len(layers)} layer(s) configured")
    except Exception as e:
        click.echo(f"Error listing layers: {str(e)}", err=True)


@config.command("remove-layer")
@click.argument("layer_name")
@click.confirmation_option(prompt="Are you sure you want to remove this layer?")
def remove_layer_command(layer_name: str) -> None:
    """Remove a layer from the configuration.

    Args:
        layer_name: Name of the layer to remove

    Examples:
        bqwizard config remove-layer raw
    """
    try:
        layer_name = layer_name.strip().lower()

        if remove_layer(layer_name):
            click.echo(f"Layer '{layer_name}' removed successfully")
        else:
            click.echo(f"Layer '{layer_name}' not found")
    except Exception as e:
        click.echo(f"Error removing layer: {str(e)}", err=True)


@config.command("get-layer")
@click.argument("layer_name")
def get_layer_command(layer_name: str) -> None:
    """Get the project ID for a specific layer.

    Args:
        layer_name: Name of the layer to lookup

    Examples:
        bqwizard config get-layer raw
    """
    try:
        layer_name = layer_name.strip().lower()
        project_id = get_layer_project(layer_name)

        if project_id:
            click.echo(f"Layer '{layer_name}': {project_id}")
        else:
            click.echo(f"Layer '{layer_name}' not configured")
    except Exception as e:
        click.echo(f"Error getting layer: {str(e)}", err=True)
