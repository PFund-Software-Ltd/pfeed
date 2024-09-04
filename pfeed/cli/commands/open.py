from pathlib import Path
import importlib.resources
import subprocess

import click

from pfeed.const.paths import PROJ_NAME, USER_CONFIG_FILE_PATH


def open_with_vscode(file_path):
    try:
        subprocess.run(["code", str(file_path)], check=True)
        click.echo(f"Opened {file_path} with VS Code")
    except subprocess.CalledProcessError:
        click.echo("Failed to open with VS Code. Falling back to default editor.")
        click.edit(filename=file_path)
    except FileNotFoundError:
        click.echo("VS Code command 'code' not found. Falling back to default editor.")
        click.edit(filename=file_path)


@click.command()
@click.option('--config-file', '-c', is_flag=True, help='Open the config file')
@click.option('--log-file', '-l', is_flag=True, help='Open the logging.yaml file for logging config')
@click.option('--docker-file', '-d', is_flag=True, help='Open the docker-compose.yml file')
@click.option('--default-editor', '-e', is_flag=True, help='Use default editor')
def open(config_file, log_file, docker_file, default_editor):
    """Opens the log file or docker-compose.yml file."""
    if all([config_file, log_file, docker_file]):
        click.echo('Please specify only one file to open')
        return
    
    package_dir = Path(importlib.resources.files(PROJ_NAME)).resolve().parents[0]
    if config_file:
        file_path = USER_CONFIG_FILE_PATH
    elif log_file:
        file_path = package_dir / 'logging.yml'
    elif docker_file:
        file_path = package_dir / 'docker-compose.yml'
    else:
        click.echo('Please specify a file to open')
        return
    
    if default_editor:
        click.edit(filename=file_path)
    else:
        open_with_vscode(file_path)