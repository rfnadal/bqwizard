import click
from pydantic import BaseModel, Field
from typing import Dict, Optional
import platformdirs
from pathlib import Path
import json
import os


class Settings(BaseModel):
    """Pydantic Model that represents the user preference settings."""

    project: str = Field(
        ..., description="The project you want to use for the current session"
    )
    layers: Dict[str, str] = Field(
        default_factory=dict,
        description="Layer to project mappings (e.g., {'raw': 'project1', 'clean': 'project2'})"
    )


def load_config(config_file_dir: str) -> Settings:
    """Loads the config file from the config file directory.
    Args:
        config_file_dir (str): The complete path to the configuration file

    Returns:
        Settings: The settings object
    """

    with open(config_file_dir, "r") as f:
        config = json.load(f)
    return Settings.model_validate_json(config)


def create_initial_config(config_file_dir: str) -> None:
    """Instantiates an empty initial config file.
    Args:
        config_file_dir (str): the complete path to the configuration file.

    Returns:
        None: Creates config file.
    """
    with open(config_file_dir, "w") as f:
        init = Settings(project="", layers={})
        data = init.model_dump_json()
        json.dump(data, f, indent=4)


def get_user_config_dir(cli="BQWIZARD") -> str:
    """Get the appropriate config directory for the current platform."""
    config_dir = Path(platformdirs.user_config_dir(cli))
    config_file_dir = f"{config_dir}/config.json"
    if os.path.exists(config_dir):
        return config_file_dir
    else:
        click.echo(
            f"Config directory does not exist. Creating Directory {config_dir} ..."
        )
        os.makedirs(config_dir, exist_ok=True)
        click.echo(f"Creating initial configuration file {config_file_dir}.")
        create_initial_config(config_file_dir)
        click.echo("Creating initial configuration file {config_file_dir}.")
        click.echo(f"Inital Config Created in {config_file_dir}")
        return config_file_dir


def update_config(config_file_dir: str, updated_settings: Settings) -> None:
    """Replaces the user config with updated parameters.
    Args:
        config_file_dir (str): the complete path to the configuration file.
        updated_settings (settings): The updated pydantic model that contains the new config.

    Returns:
        None: Replaces the config.json file with the updated user config file.
    """
    with open(config_file_dir, "w") as f:
        json.dump(updated_settings.model_dump_json(), f, indent=4)


def get_project_from_config() -> str:
    """Get the configured project from the config file.
    Args:
        None

    Returns:
        str: The configured project ID, or empty string if not set
    """
    config_file_dir = get_user_config_dir()
    settings = load_config(config_file_dir)
    return settings.project


def set_layer(layer_name: str, project_id: str) -> None:
    """Set a layer to project mapping in the config.

    Args:
        layer_name: Name of the layer (e.g., 'raw', 'clean', 'analytics')
        project_id: Project ID for this layer

    Returns:
        None: Updates the configuration file
    """
    config_file_dir = get_user_config_dir()
    settings = load_config(config_file_dir)

    # Update layers dictionary
    settings.layers[layer_name] = project_id

    # Save updated settings
    update_config(config_file_dir, settings)


def get_layer_project(layer_name: str) -> Optional[str]:
    """Get the project ID for a specific layer.

    Args:
        layer_name: Name of the layer to lookup

    Returns:
        str: Project ID for the layer, or None if not configured
    """
    config_file_dir = get_user_config_dir()
    settings = load_config(config_file_dir)
    return settings.layers.get(layer_name)


def get_all_layers() -> Dict[str, str]:
    """Get all configured layers.

    Returns:
        Dict[str, str]: Dictionary mapping layer names to project IDs
    """
    config_file_dir = get_user_config_dir()
    settings = load_config(config_file_dir)
    return settings.layers.copy()


def remove_layer(layer_name: str) -> bool:
    """Remove a layer from the configuration.

    Args:
        layer_name: Name of the layer to remove

    Returns:
        bool: True if layer was removed, False if it didn't exist
    """
    config_file_dir = get_user_config_dir()
    settings = load_config(config_file_dir)

    if layer_name in settings.layers:
        del settings.layers[layer_name]
        update_config(config_file_dir, settings)
        return True
    return False
