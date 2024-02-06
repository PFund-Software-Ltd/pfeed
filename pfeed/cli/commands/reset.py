import click

from pfeed.const.paths import USER_CONFIG_FILE_PATH
from pfeed.cli.commands.config import remove_config


@click.command()
def reset():
    """Resets the application configuration by removing the existing configuration file."""
    remove_config(USER_CONFIG_FILE_PATH)
    click.echo("Configuration successfully reset.")
