from pathlib import Path

import click


@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.pass_context
@click.option('--docker-file', 'docker_compose_file_path', type=click.Path(exists=True), help='Path to the docker-compose.yml file')
def docker_compose(ctx, docker_compose_file_path):
    """Forwards commands to docker-compose with the package's docker-compose.yml file if not specified."""
    import os
    import subprocess
    
    config = ctx.obj['config']
    docker_compose_file_path = docker_compose_file_path or config.docker_compose_file_path
    click.echo(f'Using docker-compose.yml file from "{docker_compose_file_path}"')
    command = ['docker-compose', '--file', str(docker_compose_file_path)] + ctx.args
    
    data_path = Path(config.data_path).parent
    if 'MINIO_DATA_PATH' not in os.environ:
        os.environ['MINIO_DATA_PATH'] = str(data_path / 'minio')  # used in docker-compose.yml
    if 'TIMESCALEDB_DATA_PATH' not in os.environ:
        os.environ['TIMESCALEDB_DATA_PATH'] = str(data_path / 'timescaledb')  # used in docker-compose.yml
    subprocess.run(command, env=os.environ)
