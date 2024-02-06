import os
import sys
import yaml
from pathlib import Path
from pprint import pformat

import click

from pfeed.const.paths import USER_CONFIG_FILE_PATH
from pfeed.config_handler import ConfigHandler


def load_config(config_file_path: str | Path):
    config_file_path = Path(config_file_path)
    if config_file_path.is_file():
        with open(config_file_path, 'r') as f:
            return yaml.safe_load(f) or {}
    return {}


def save_config(config: ConfigHandler, config_file_path: str | Path):
    with open(config_file_path, 'w') as f:
        yaml.dump(config.__dict__, f, default_flow_style=False)
        

def remove_config(config_file_path: str | Path):
    os.remove(config_file_path)


@click.command()
@click.pass_context
@click.option('--data-path', type=click.Path(), help='Set the data path')
@click.option('--log-path', type=click.Path(), help='Set the log path')
@click.option('--logging-path', 'logging_config_file_path', type=click.Path(exists=True), help='Set the logging config file path')
@click.option('--logging-config', type=dict, help='Set the logging config')
@click.option('--use-fork-process', type=bool, help='If True, multiprocessing.set_start_method("fork")')
@click.option('--use-custom-excepthook', type=bool, help='If True, log uncaught exceptions to file')
def config(ctx, **kwargs):
    """Configures pfeed settings."""
    config: ConfigHandler = ctx.obj['config']
    
    # Filter out options that were not provided by the user
    provided_options = {k: v for k, v in kwargs.items() if v is not None}
    if not provided_options:
        click.echo(f"PFeed's config:\n{pformat(config.__dict__)}")
        sys.exit(1)

    for option, value in provided_options.items():
        setattr(config, option, value)
        click.echo(f"{option} set to: {value}")
    
    save_config(config, USER_CONFIG_FILE_PATH)
    click.echo(f"config saved to {USER_CONFIG_FILE_PATH}.")
