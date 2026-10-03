import datetime
import os
from pathlib import Path

import click
from pfund_kit.style import RichColor, TextStyle, cprint

from pfeed.io.ducklake_io import DuckLakeIO

_BASE_PATH_CONTEXT_KEY = 'pfeed.ducklake.base_path'


@click.group()
@click.option(
    '--base-path',
    type=click.Path(path_type=Path, file_okay=False),
    help=f'Override the DuckLake base path (default: <data_path>/{DuckLakeIO.DEFAULT_DIR_NAME})',
)
@click.pass_context
def ducklake(ctx: click.Context, base_path: Path | None):
    """Maintain the DuckLake tables."""
    ctx.meta[_BASE_PATH_CONTEXT_KEY] = base_path


def _get_io(ctx: click.Context) -> DuckLakeIO:
    """Not created in the group, since a missing catalog would then also fail `<command> --help`."""
    base_path: Path | None = ctx.meta[_BASE_PATH_CONTEXT_KEY]
    if base_path is None:
        from pfeed.config import get_config

        base_path = get_config().data_path / DuckLakeIO.DEFAULT_DIR_NAME
    # DuckLakeIO would create an empty catalog, which maintaining is pointless
    if not os.path.exists(base_path / DuckLakeIO.CATALOG_FILE_NAME):
        raise click.ClickException(f'No DuckLake catalog found under {base_path}')
    return DuckLakeIO(base_path=str(base_path))


@ducklake.command()
@click.pass_context
def optimize(ctx: click.Context):
    """Compacts all tables into fewer, larger parquet files to speed up reads.

    - moves inlined rows (small writes stored in the catalog) into parquet files
    - rewrites files where most rows were deleted
    - merges small files into larger ones

    **Best Practice: Run `vacuum` After `optimize`**
    - `optimize` only adds files, the replaced ones stay on disk for old snapshots.
    - `vacuum` then deletes them once they are older than the retention period.
    """
    io = _get_io(ctx)
    cprint('Optimizing DuckLake...', style=TextStyle.BOLD + RichColor.YELLOW)
    io.optimize()
    cprint('Optimized DuckLake', style=TextStyle.BOLD + RichColor.GREEN)


@ducklake.command()
@click.option('--no-dry-run', '-n', is_flag=True, help='Actually expire snapshots and delete files')
@click.option(
    '--retention-hours',
    '-h',
    type=click.IntRange(min=0),
    default=int(DuckLakeIO.DEFAULT_VACUUM_RETENTION.total_seconds() // 3600),
    show_default=True,
    help='Number of hours of history to keep',
)
@click.pass_context
def vacuum(ctx: click.Context, no_dry_run: bool, retention_hours: int):
    """Deletes snapshots and files older than the retention period.

    - expires snapshots older than the retention period (time travel to them is no longer possible)
    - deletes files no longer used by any snapshot, and files left by crashed writes

    **Dry Run Mode (Default):**
    - By default, NOTHING is expired or deleted, it only lists what would be.
    - To actually expire and delete, use the `--no-dry-run` (`-n`) flag.
    """
    io = _get_io(ctx)
    dry_run = not no_dry_run
    if dry_run:
        cprint(
            'This is a dry run. NOTHING will actually be deleted. To turn it off, use the --no-dry-run/-n flag.',
            style=TextStyle.BOLD + RichColor.YELLOW,
        )
    snapshot_ids, paths = io.vacuum(retention=datetime.timedelta(hours=retention_hours), dry_run=dry_run)
    expire, delete = ('Going to expire', 'Going to delete') if dry_run else ('Expired', 'Deleted')
    color = RichColor.RED if dry_run else RichColor.GREEN
    cprint(f'{expire} {len(snapshot_ids)} snapshot(s)', style=TextStyle.BOLD + color)
    cprint(f'{delete} {len(paths)} file(s)', style=TextStyle.BOLD + color)
    for path in paths:
        cprint(f'  {path}', style=color)
