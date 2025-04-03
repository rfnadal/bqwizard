import click
from click import Context
from .utils.config_utils import get_user_config_dir, load_config, update_config


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
