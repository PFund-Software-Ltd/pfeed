import click

from pfund import cprint
from pfeed.enums import DataStorage
from pfeed.storages.base_storage import BaseStorage



@click.group()
def deltalake():
    pass


@deltalake.command()
@click.option('--storage', '-s', type=click.Choice(DataStorage, case_sensitive=False), required=True, help='Storage to vacuum')
@click.option('--no-dry-run', '-n', is_flag=True, help='Actually delete files')
def vacuum(storage: DataStorage, no_dry_run: bool):
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
    '''
    if not no_dry_run:
        cprint('This is a dry run. NO files will actually be deleted. To turn it off, use the --no-dry-run/-n flag.', style='bold yellow')
    Storage = storage.storage_class
    storage: BaseStorage = Storage(use_deltalake=True)
    dry_run = not no_dry_run
    storage.vacuum_delta_files(dry_run=dry_run)


@deltalake.command()
@click.option('--storage', '-s', type=click.Choice(DataStorage, case_sensitive=False), required=True, help='Storage to optimize')
def optimize(storage: DataStorage):
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
    '''
    Storage = storage.storage_class
    storage: BaseStorage = Storage(use_deltalake=True)
    storage.optimize_delta_files()
