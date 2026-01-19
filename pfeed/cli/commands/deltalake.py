import click

from pfund_kit.style import cprint
from pfeed.enums import DataStorage
from pfeed.storages.base_storage import BaseStorage



@click.group()
def deltalake():
    pass


@deltalake.command()
@click.option('--storage', '-s', type=click.Choice(DataStorage, case_sensitive=False), required=True, help='Storage to vacuum')
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
    Storage = storage.storage_class
    storage: BaseStorage = Storage()
    dry_run = not no_dry_run
    enforce_retention_duration = not no_enforce_retention_duration
    storage.vacuum_delta_files(
        dry_run=dry_run,
        retention_hours=retention_hours,
        enforce_retention_duration=enforce_retention_duration
    )


@deltalake.command()
@click.option('--storage', '-s', type=click.Choice(DataStorage, case_sensitive=False), required=True, help='Storage to optimize')
@click.option('--partition-filter', '-p', multiple=True, help='Partition filter in format "column=value"')
@click.option('--target-size', '-t', type=int, help='Desired file size after compaction in bytes (default: 256MB)')
@click.option('--max-concurrent-tasks', '-n', type=int, help='Maximum number of concurrent tasks')
@click.option('--min-commit-interval', '-i', type=int, help='Minimum interval before creating a new commit (in seconds)')
@click.option('--no-print', is_flag=True, help='Disable printing output')
def optimize(storage: DataStorage, partition_filter: tuple, target_size: int = None, 
             max_concurrent_tasks: int = None, min_commit_interval: int = None, no_print: bool = False):
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
    - Target specific partitions using `-p column=value` (can be used multiple times)
    - Example: `-p year=2023 -p month=1` for Jan 2023 data
    
    **Advanced Options:**
    - Use `--target-size` to specify desired file size after compaction (in bytes)
    - Control parallelism with `--max-concurrent-tasks`
    - Set `--min-commit-interval` (seconds) for long-running operations
    '''
    Storage = storage.storage_class
    storage: BaseStorage = Storage()
    
    # Parse partition filters if provided
    partition_filters = None
    if partition_filter:
        partition_filters = []
        for filt in partition_filter:
            if '=' in filt:
                col, val = filt.split('=', 1)
                partition_filters.append((col, '=', val))
    
    storage.optimize_delta_files(
        partition_filters=partition_filters,
        target_size=target_size,
        max_concurrent_tasks=max_concurrent_tasks,
        min_commit_interval=min_commit_interval,
        print_output=not no_print
    )
