"""`pfeed ducklake|deltalake|iceberg optimize|vacuum`: the same maintenance commands for every TableIO."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfeed.io.table_io import TableIO

    type OptionDecorator = Callable[[Callable[..., Any]], Callable[..., Any]]

import datetime
import importlib
import os
from pathlib import Path

import click
from pfund_kit.style import RichColor, TextStyle, cprint

_BASE_PATH_CONTEXT_KEY = 'pfeed.table_io.base_path'
_DEFAULT_RETENTION_HOURS = 7 * 24  # TableIO.DEFAULT_VACUUM_RETENTION, not imported to keep the CLI startup light


def _resolve_pfeed_base_path(dir_name: str, **_: Any) -> Path:
    from pfeed.config import get_config

    return get_config().data_path / dir_name


def create_table_io_command(
    name: str,
    io_class_path: str,
    *,
    title: str,
    install_extra: str | None = None,
    base_path_resolver: Callable[..., Path] = _resolve_pfeed_base_path,
    extra_options: tuple[OptionDecorator, ...] = (),
) -> click.Group:
    """Creates the maintenance command group of a TableIO.

    Args:
        name: name of the command group, e.g. 'ducklake'.
        io_class_path: '<module>:<class>' of the TableIO, imported only when a command runs,
            so the CLI works without the optional dependencies of the other IOs.
        title: name of the table format in messages, e.g. 'DuckLake'.
        install_extra: pfeed extra installing the IO's optional dependency, e.g. 'deltalake'.
        base_path_resolver: returns the default base path, given the IO's DEFAULT_DIR_NAME as `dir_name`
            and the values of `extra_options` as keyword args; e.g. pfund resolves its own data path.
        extra_options: click options added to the group.
    """

    def load_io_class() -> type[TableIO]:
        module_name, class_name = io_class_path.split(':')
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            install = f", install it with: pip install 'pfeed[{install_extra}]'" if install_extra else ''
            raise click.ClickException(f'{title} is not installed ({e}){install}') from None
        return getattr(module, class_name)

    def get_io(ctx: click.Context) -> TableIO:
        """Not created in the group, since a missing base path would then also fail `<command> --help`."""
        io_class = load_io_class()
        base_path, resolver_kwargs = ctx.meta[_BASE_PATH_CONTEXT_KEY]
        if base_path is None:
            base_path = base_path_resolver(dir_name=io_class.DEFAULT_DIR_NAME, **resolver_kwargs)
        # the IO would create an empty lake, which maintaining is pointless
        if not os.path.isdir(base_path):
            raise click.ClickException(f'No {title} tables found under {base_path}')
        return io_class(base_path=str(base_path))

    def group_callback(ctx: click.Context, base_path: Path | None, **resolver_kwargs: Any) -> None:
        ctx.meta[_BASE_PATH_CONTEXT_KEY] = (base_path, resolver_kwargs)

    callback: Callable[..., Any] = click.pass_context(group_callback)
    callback = click.option(
        '--base-path',
        type=click.Path(path_type=Path, file_okay=False),
        help=f'Override the {title} base path (default: <data_path>/{name})',
    )(callback)
    for option in extra_options:
        callback = option(callback)
    group = click.group(name=name, help=f'Maintain the {title} tables.')(callback)

    @group.command()
    @click.pass_context
    def optimize(ctx: click.Context):
        """Compacts all tables into fewer, larger parquet files to speed up reads.

        **Best Practice: Run `vacuum` After `optimize`**
        - `optimize` only adds files, the replaced ones stay on disk for old versions.
        - `vacuum` then deletes them once they are older than the retention period.
        """
        io = get_io(ctx)
        cprint(f'Optimizing {title}...', style=TextStyle.BOLD + RichColor.YELLOW)
        io.optimize()
        cprint(f'Optimized {title}', style=TextStyle.BOLD + RichColor.GREEN)

    @group.command()
    @click.option('--no-dry-run', '-n', is_flag=True, help='Actually delete files')
    @click.option(
        '--retention-hours',
        '-h',
        type=click.IntRange(min=0),
        default=_DEFAULT_RETENTION_HOURS,
        show_default=True,
        help='Number of hours of history to keep',
    )
    @click.pass_context
    def vacuum(ctx: click.Context, no_dry_run: bool, retention_hours: int):
        """Deletes the history older than the retention period.

        - expires snapshots older than the retention period (DuckLake, Iceberg; Delta Lake does it itself)
        - deletes files old versions used, once they stopped being used longer ago than the retention period
        - deletes files no version ever used (e.g. left by crashed writes), once older than the retention period
        - time travel to versions older than the retention period is no longer possible

        **Dry Run Mode (Default):**
        - By default, NOTHING is deleted, it only lists what would be.
        - To actually delete, use the `--no-dry-run` (`-n`) flag.
        """
        io = get_io(ctx)
        dry_run = not no_dry_run
        if dry_run:
            cprint(
                'This is a dry run. NOTHING will actually be deleted. To turn it off, use the --no-dry-run/-n flag.',
                style=TextStyle.BOLD + RichColor.YELLOW,
            )
        result = io.vacuum(retention=datetime.timedelta(hours=retention_hours), dry_run=dry_run)
        color = RichColor.RED if dry_run else RichColor.GREEN
        expire, delete = ('Going to expire', 'Going to delete') if dry_run else ('Expired', 'Deleted')
        for verb, noun, items in (
            (expire, 'snapshot(s)', result.expired_snapshots), (delete, 'file(s)', result.deleted_paths),
        ):
            cprint(f'{verb} {len(items)} {noun}', style=TextStyle.BOLD + color)
            for item in items:
                cprint(f'  {item}', style=color)

    return group


ducklake = create_table_io_command('ducklake', 'pfeed.io.ducklake_io:DuckLakeIO', title='DuckLake')
deltalake = create_table_io_command(
    'deltalake', 'pfeed.io.deltalake_io:DeltaLakeIO', title='Delta Lake', install_extra='deltalake',
)
iceberg = create_table_io_command('iceberg', 'pfeed.io.iceberg_io:IcebergIO', title='Iceberg', install_extra='iceberg')
