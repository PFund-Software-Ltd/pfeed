from pathlib import Path
import re

import click
from deltalake import DeltaTable

from pfund_kit.style import cprint, TextStyle, RichColor
from pfeed.enums import DataStorage, IOFormat
from pfeed.storages.file_based_storage import FileBasedStorage


# Supported partition filter operators (order matters: check multi-char operators first)
PARTITION_FILTER_OPERATORS = ['>=', '<=', '!=', '>', '<', '=']
PARTITION_FILTER_PATTERN = re.compile(r'^(\w+)(>=|<=|!=|>|<|=)(.+)$')


def parse_partition_filter(filt: str) -> tuple[str, str, str] | None:
    """
    Parse a partition filter string into (column, operator, value) tuple.

    Supports operators: =, !=, <, <=, >, >=

    Args:
        filt: Filter string like "column=value" or "column>=10"

    Returns:
        Tuple of (column, operator, value) or None if parsing fails.
    """
    match = PARTITION_FILTER_PATTERN.match(filt)
    if match:
        return (match.group(1), match.group(2), match.group(3))
    return None


def discover_delta_tables(data_path: str | Path) -> list[Path]:
    """
    Discover all Delta Lake tables under the given data path.

    Delta tables are identified by the presence of a `_delta_log` directory.

    Args:
        data_path: The root path to scan for delta tables.

    Returns:
        A list of paths to delta tables (parent directories of _delta_log).
    """
    data_path = Path(data_path)
    if not data_path.exists():
        return []
    # Find all _delta_log directories and return their parent (the table path)
    return sorted([p.parent for p in data_path.rglob('_delta_log') if p.is_dir()])


@click.group()
def deltalake():
    pass


@deltalake.command()
@click.option('--storage', '-s', type=click.Choice([DataStorage.CACHE, DataStorage.LOCAL], case_sensitive=False), required=True, help='Storage to vacuum')
@click.option('--no-dry-run', '-n', is_flag=True, help='Actually delete files')
@click.option('--retention-hours', '-h', type=int, help='Number of hours to retain files')
@click.option('--no-enforce-retention-duration', '--ne', is_flag=True, help='Disable enforcement of retention duration')
def vacuum(storage: DataStorage, no_dry_run: bool, retention_hours: int = None, no_enforce_retention_duration: bool = False):
    '''Cleans up old, unreferenced files in Delta Lake tables.

    This command removes obsolete files that are no longer needed by Delta Lake, freeing up storage space and improving query performance.

    **Best Practice: Run `VACUUM` After `OPTIMIZE`**
    - `OPTIMIZE` first merges small files into larger ones for better performance.
    - `VACUUM` then removes old, unreferenced files from previous table versions.

    **Key Features:**
    - Deletes files that are no longer referenced in the Delta transaction log.
    - Helps reclaim storage by removing outdated versions of Delta tables.
    - Reduces the number of files scanned during queries.

    **When to Run This Command:**
    - After large data updates, deletions, or merges that generate many new files.
    - When storage needs to be optimized by removing old data versions.
    - As part of regular maintenance (e.g., weekly or monthly).

    **Dry Run Mode (Default):**
    - By default, this command runs in "dry run" mode, meaning **no files are actually deleted**.
    - To actually delete unreferenced files, use the `--no-dry-run` (`-n`) flag.

    **Retention Hours:**
    - Specifies how many hours of history to retain (default determined by Delta Lake).
    - Files older than this threshold will be removed if not referenced.

    **Enforce Retention Duration:**
    - By default, Delta Lake enforces a minimum retention period for safety.
    - Use `--no-enforce-retention-duration` to disable this safety check (use with caution).
    '''
    if not no_dry_run:
        cprint('This is a dry run. NO files will actually be deleted. To turn it off, use the --no-dry-run/-n flag.', style='bold yellow')

    # Get storage class and its data_path
    Storage = storage.storage_class
    assert issubclass(Storage, FileBasedStorage), f'{Storage} is not a subclass of {FileBasedStorage}, which doesn\'t support Delta Lake'
    storage = Storage()
    storage.with_io(io_format=IOFormat.DELTALAKE)
    data_path = storage.data_path

    # Discover all delta tables under this storage
    delta_tables = discover_delta_tables(str(data_path))

    if not delta_tables:
        cprint(f'No Delta Lake tables found under {data_path}', style=TextStyle.BOLD + RichColor.YELLOW)
        return

    cprint(f'Found {len(delta_tables)} Delta Lake table(s) under {data_path}', style=TextStyle.BOLD + RichColor.BLUE)

    dry_run = not no_dry_run
    enforce_retention_duration = not no_enforce_retention_duration

    for table_path in delta_tables:
        try:
            delta_table = DeltaTable(str(table_path))
        except Exception as e:
            cprint(f'Failed to open Delta table at {table_path}: {e}', style=TextStyle.BOLD + RichColor.RED)
            continue
        storage.vacuum_delta_table(
            delta_table,
            dry_run=dry_run,
            retention_hours=retention_hours,
            enforce_retention_duration=enforce_retention_duration,
        )


@deltalake.command()
@click.option('--storage', '-s', type=click.Choice([DataStorage.CACHE, DataStorage.LOCAL], case_sensitive=False), required=True, help='Storage to optimize', default=DataStorage.LOCAL)
@click.option('--partition-filter', '-p', multiple=True, help='Partition filter in format "column<op>value" where op is =, !=, <, <=, >, >=')
@click.option('--target-size', '-t', type=int, help='Desired file size after compaction in bytes (default: 100MB)')
@click.option('--max-concurrent-tasks', '-m', type=int, help='Maximum number of concurrent tasks')
@click.option('--min-commit-interval', '-i', type=int, help='Minimum interval before creating a new commit (in seconds)')
def optimize(storage: DataStorage, partition_filter: tuple, target_size: int = None,
             max_concurrent_tasks: int = None, min_commit_interval: int = None):
    '''
    Merges small files in Delta Lake tables into larger, more efficient files.

    This command improves query performance and reduces metadata overhead by compacting fragmented Parquet files.

    **Key Features:**
    - Combines small files into fewer, larger files to speed up queries.
    - Reduces the number of files scanned during read operations.
    - Helps optimize storage layout for better performance.

    **Best Practice: Run `VACUUM` After `OPTIMIZE`**
    - `OPTIMIZE` merges files but does **not** delete old ones.
    - Running `VACUUM` after `OPTIMIZE` permanently removes unreferenced files.

    **When to Run This Command:**
    - After frequent updates, inserts, or deletes that create many small files.
    - Before running heavy analytical queries to improve performance.
    - As part of regular table maintenance (e.g., weekly or monthly).

    **Partition Filters:**
    - Target specific partitions using `-p column<op>value` (can be used multiple times)
    - Supported operators: =, !=, <, <=, >, >=
    - Example: `-p year=2023 -p month>=1` for data from Jan 2023 onwards

    **Advanced Options:**
    - Use `--target-size` to specify desired file size after compaction (in bytes)
    - Control parallelism with `--max-concurrent-tasks`
    - Set `--min-commit-interval` (seconds) for long-running operations
    '''
    # Get storage class and its data_path
    Storage = storage.storage_class
    assert issubclass(Storage, FileBasedStorage), f'{Storage} is not a subclass of {FileBasedStorage}, which doesn\'t support Delta Lake'
    storage = Storage()
    storage.with_io(io_format=IOFormat.DELTALAKE)
    data_path = storage.data_path

    # Discover all delta tables under this storage
    delta_tables = discover_delta_tables(str(data_path))

    if not delta_tables:
        cprint(f'No Delta Lake tables found under {data_path}', style=TextStyle.BOLD + RichColor.YELLOW)
        return

    cprint(f'Found {len(delta_tables)} Delta Lake table(s) under {data_path}', style=TextStyle.BOLD + RichColor.BLUE)

    # Parse partition filters if provided
    partition_filters = None
    if partition_filter:
        partition_filters = []
        for filt in partition_filter:
            parsed = parse_partition_filter(filt)
            if parsed:
                partition_filters.append(parsed)
            else:
                cprint(f'Invalid partition filter format: "{filt}". Expected format: column<op>value (op: =, !=, <, <=, >, >=)', style=TextStyle.BOLD + RichColor.RED)
                return

    for table_path in delta_tables:
        try:
            delta_table = DeltaTable(str(table_path))
        except Exception as e:
            cprint(f'Failed to open Delta table at {table_path}: {e}', style=TextStyle.BOLD + RichColor.RED)
            continue
        storage.optimize_delta_table(
            delta_table,
            partition_filters=partition_filters,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            min_commit_interval=min_commit_interval,
        )
