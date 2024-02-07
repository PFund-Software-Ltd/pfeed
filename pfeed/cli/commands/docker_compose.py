from pathlib import Path
import importlib.resources
import subprocess

from dotenv import find_dotenv
import click

from pfeed.const.paths import PROJ_NAME


@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.pass_context
def docker_compose(ctx):
    """Forwards commands to docker-compose with the package's docker-compose.yml file."""
    package_dir = Path(importlib.resources.files(PROJ_NAME)).resolve().parents[0]
    env_file_path = find_dotenv(usecwd=True, raise_error_if_not_found=True)
    click.echo(f'env file: {env_file_path}')
    docker_compose_file = package_dir / 'docker-compose.yml'

    command = ['docker-compose', '-f', str(docker_compose_file)] + ctx.args
    subprocess.run(command)



