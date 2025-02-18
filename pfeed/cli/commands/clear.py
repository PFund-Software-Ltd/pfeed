import os
import shutil
from pathlib import Path

import click


@click.group()
def clear():
    """Clear all caches, data and logs."""
    pass


@clear.command()
@click.pass_context
def cache(ctx):
    """Clear all caches."""
    config = ctx.obj['config']
    cache_path = config.cache_path
    try:
        click.echo(f"Clearing all caches in {cache_path}...")
        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)
            os.makedirs(cache_path, exist_ok=True)  # Recreate the empty cache directory
        else:
            click.echo("No cache directory found")
        click.echo("Cache cleared successfully!")
    except Exception as e:
        click.echo(f"Error clearing cache: {str(e)}", err=True)


@clear.command()
@click.pass_context
@click.option('--target', '-t', type=click.Choice(['all', 'minio'], case_sensitive=True), required=True, help='The target data to clear.')
def data(ctx, target):
    """Clear all data in the data directory."""
    config = ctx.obj['config']
    if target == 'all':
        data_path = config.data_path
    elif target == 'minio':
        if 'MINIO_DATA_PATH' in os.environ:
            data_path = os.getenv('MINIO_DATA_PATH')
        else:
            data_path = str(Path(config.data_path).parent / 'minio')
    else:
        raise ValueError(f"Invalid target: {target}")
    try:
        click.echo(f"Clearing all data in {data_path}...")
        if os.path.exists(data_path):
            shutil.rmtree(data_path)
            os.makedirs(data_path, exist_ok=True)
        else:
            click.echo("No data directory found")
        click.echo("Data cleared successfully!")
    except Exception as e:
        click.echo(f"Error clearing data: {str(e)}", err=True)


@clear.command()
@click.pass_context
def log(ctx):
    """Clear all logs."""
    config = ctx.obj['config']
    log_path = config.log_path
    try:
        click.echo(f"Clearing all logs in {log_path}...")
        if os.path.exists(log_path):
            shutil.rmtree(log_path)
            os.makedirs(log_path, exist_ok=True)
        else:
            click.echo("No log directory found")
        click.echo("Logs cleared successfully!")
    except Exception as e:
        click.echo(f"Error clearing logs: {str(e)}", err=True)
