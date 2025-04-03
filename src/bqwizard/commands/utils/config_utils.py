import click
from pydantic import BaseModel, Field
import platformdirs
from pathlib import Path
import json
import os


class Settings(BaseModel):
    """Pydatinc Model that represents the user preference settings."""

    project: str = Field(
        ..., description="The project you want to use for the current session"
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
        init = Settings(project="")
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
