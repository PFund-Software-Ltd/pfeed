import os
import yaml
from pathlib import Path
from pprint import pformat

import click

from pfeed.const.paths import USER_CONFIG_FILE_PATH
from pfeed.config_handler import ConfigHandler


def save_config(config: ConfigHandler, config_file_path: str | Path):
    if isinstance(config_file_path, str):
        config_file_path = Path(config_file_path)
    config_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_file_path, 'w') as f:
        yaml.dump(config.__dict__, f, default_flow_style=False)
        

def remove_config(config_file_path: str | Path):
    config_file_path = Path(config_file_path)
    if config_file_path.is_file():
        os.remove(config_file_path)


@click.command()
@click.pass_context
@click.option('--data-path', type=click.Path(resolve_path=True), help='Set the data path')
@click.option('--log-path', type=click.Path(resolve_path=True), help='Set the log path')
@click.option('--logging-file', 'logging_config_file_path', type=click.Path(resolve_path=True, exists=True), help='Set the logging config file path')
@click.option('--logging-config', type=dict, help='Set the logging config')
@click.option('--use-fork-process', type=bool, help='If True, multiprocessing.set_start_method("fork")')
@click.option('--use-custom-excepthook', type=bool, help='If True, log uncaught exceptions to file')
@click.option('--env-file', 'env_file_path', type=click.Path(resolve_path=True, exists=True), help='Path to the .env file')
@click.option('--debug', '-d', type=bool, help='If True, enable debug mode where logs at DEBUG level will be printed')
@click.option('--list', '-l', 'is_list', is_flag=True, is_eager=True, help='List all available options')
@click.option('--reset', 'is_reset', is_flag=True, is_eager=True, help='Reset the configuration to defaults') 
def config(ctx, is_list, is_reset, **kwargs):
    """Configures pfeed settings."""
    config: ConfigHandler = ctx.obj['config']
    
    # Filter out options that were not provided by the user
    provided_options = {k: v for k, v in kwargs.items() if v is not None}
    
    if is_list:  # Check if --list was used
        assert not provided_options, "No options should be provided with --list"
        config_dict = config.__dict__
        config_dict.update({'config_file_path': USER_CONFIG_FILE_PATH})
        click.echo(f"PFeed's config:\n{pformat(config_dict)}")
        return

    if is_reset:  # Check if --reset was used
        assert not provided_options, "No options should be provided with --reset"
        remove_config(USER_CONFIG_FILE_PATH)
        click.echo("PFeed's config successfully reset.")
    
    # prints out current config if no options are provided
    if not provided_options and not is_list and not is_reset:
        raise click.UsageError("No options provided. Use --list to see all available options.")
    else:
        for option, value in provided_options.items():
            setattr(config, option, value)
            click.echo(f"{option} set to: {value}")
        
        save_config(config, USER_CONFIG_FILE_PATH)
        click.echo(f"config saved to {USER_CONFIG_FILE_PATH}.")
