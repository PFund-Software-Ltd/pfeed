from pfund_kit.cli import create_cli_group
from pfund_kit.cli.commands import config, docker_compose, remove
from pfeed.cli.commands.deltalake import deltalake
# TODO
# from pfund_kit.cli.commands import doc
# from pfeed.cli.commands.download import download
# from pfeed.cli.commands.stream import stream


def init_context(ctx):
    """Initialize pfeed-specific context"""
    from pfeed.config import get_config
    ctx.obj['config'] = get_config()


pfeed_group = create_cli_group('pfeed', init_context=init_context)
pfeed_group.add_command(config)
pfeed_group.add_command(docker_compose)
pfeed_group.add_command(docker_compose, name='compose')
pfeed_group.add_command(remove)
pfeed_group.add_command(remove, name='rm')
pfeed_group.add_command(deltalake)
pfeed_group.add_command(deltalake, name='delta')  # alias for deltalake
# pfeed_group.add_command(doc)
# pfeed_group.add_command(download)
# pfeed_group.add_command(stream)