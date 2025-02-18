import click
from trogon import tui

from pfeed.config import get_config
from pfeed.cli.commands.docker_compose import docker_compose
from pfeed.cli.commands.config import config
from pfeed.cli.commands.download import download
# from pfeed.cli.commands.stream import stream
from pfeed.cli.commands.doc import doc
from pfeed.cli.commands.deltalake import deltalake
from pfeed.cli.commands.clear import clear


@tui(command='tui', help="Open terminal UI")
@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
@click.version_option()
def pfeed_group(ctx):
    """PFeed's CLI"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = get_config(verbose=False)


pfeed_group.add_command(docker_compose)
pfeed_group.add_command(config)
pfeed_group.add_command(download)
# pfeed_group.add_command(stream)
pfeed_group.add_command(doc)
pfeed_group.add_command(clear)
pfeed_group.add_command(deltalake)
pfeed_group.add_command(deltalake, name='delta')  # alias for deltalake