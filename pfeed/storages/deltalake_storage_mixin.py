from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed.storages.file_based_storage import FileBasedStorage
    from deltalake.table import FilterConjunctionType
    from deltalake.transaction import (
        CommitProperties,
        PostCommitHookProperties,
    )
    from deltalake.writer.properties import WriterProperties

from datetime import timedelta

from rich.pretty import Pretty
from rich.panel import Panel
from deltalake import DeltaTable

from pfund_kit.style import cprint, TextStyle, RichColor



class DeltaLakeStorageMixin:
    def get_delta_table(self: FileBasedStorage | DeltaLakeStorageMixin) -> DeltaTable:
        table_path = self.data_handler._table_path
        return self.io.get_table(table_path)
    
    def vacuum_delta_table(
        self: FileBasedStorage | DeltaLakeStorageMixin, 
        retention_hours: int | None = None,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
        full: bool = False,
        keep_versions: list[int] | None = None,
    ) -> list[str]:
        """
        Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold.

        Args:
            retention_hours: the retention threshold in hours, if none then the value from `delta.deletedFileRetentionDuration` is used or default of 1 week otherwise.
            dry_run: when activated, list only the files, delete otherwise
            enforce_retention_duration: when disabled, accepts retention hours smaller than the value from `delta.deletedFileRetentionDuration`.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.
            full: when set to True, will perform a "full" vacuum and remove all files not referenced in the transaction log
            keep_versions: An optional list of versions to keep. If provided, files from these versions will not be deleted.
        Returns:
            the list of files no longer referenced by the Delta Table and are older than the retention threshold.
        """
        cprint('Vacuuming Delta Lake...', style=TextStyle.BOLD + RichColor.YELLOW)
        delta_table = self.get_delta_table()
        files_deleted = delta_table.vacuum(
            dry_run=dry_run, 
            retention_hours=retention_hours,
            enforce_retention_duration=enforce_retention_duration,
            post_commithook_properties=post_commithook_properties,
            commit_properties=commit_properties,
            full=full,
            keep_versions=keep_versions,
        )
        table_uri = delta_table.table_uri
        verb = 'Going to delete' if dry_run else 'Deleted'
        style = TextStyle.BOLD + RichColor.RED if dry_run else TextStyle.BOLD + RichColor.GREEN
        if files_deleted:
            # Print the table URI with appropriate style
            cprint(f"{verb} {len(files_deleted)} files from {table_uri}:", style=style)
            
            # Create a pretty-formatted version of the files list
            pretty_files = Pretty(files_deleted, expand_all=True)
            
            # Display the files in a panel with a border
            cprint(Panel(
                pretty_files,
                border_style=style.replace(TextStyle.BOLD+' ', ''),
                title="Files to be deleted" if dry_run else "Deleted files",
                title_align="left"
            ))
        else:
            cprint(f'No files to delete from {table_uri}', style=TextStyle.BOLD + RichColor.BLUE)
        return files_deleted
    
    def optimize_delta_table(
        self: FileBasedStorage | DeltaLakeStorageMixin,
        partition_filters: FilterConjunctionType | None = None,
        target_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        max_spill_size: int | None = None,
        max_temp_directory_size: int | None = None,
        min_commit_interval: int | timedelta | None = None,
        writer_properties: WriterProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """
        Compacts small files to reduce the total number of files in the table.

        This operation is idempotent; if run twice on the same table (assuming it has
        not been updated) it will do nothing the second time.

        If this operation happens concurrently with any operations other than append,
        it will fail.

        Args:
            partition_filters: the partition filters that will be used for getting the matched files
            target_size: desired file size after bin-packing files, in bytes. If not
                            provided, will attempt to read the table configuration value ``delta.targetFileSize``.
                            If that value isn't set, will use default value of 100MB.
            max_concurrent_tasks: the maximum number of concurrent tasks to use for
                                    file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
                                    faster, but will also use more memory.
            max_spill_size: the maximum number of bytes allowed in memory before spilling to disk. If not specified, uses DataFusion's default.
            max_temp_directory_size: the maximum disk space for temporary spill files. If not specified, uses DataFusion's default.
            min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
                                    created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
                                    want a commit per partition.
            writer_properties: Pass writer properties to the Rust parquet writer.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from optimize

        Example:
            Use a timedelta object to specify the seconds, minutes or hours of the interval.
            ```python
            from deltalake import DeltaTable, write_deltalake
            from datetime import timedelta
            import pyarrow as pa

            write_deltalake("tmp", pa.table({"x": [1], "y": [4]}))
            write_deltalake("tmp", pa.table({"x": [2], "y": [5]}), mode="append")

            dt = DeltaTable("tmp")
            time_delta = timedelta(minutes=10)
            dt.optimize.compact(min_commit_interval=time_delta)
            {'numFilesAdded': 1, 'numFilesRemoved': 2, 'filesAdded': ..., 'filesRemoved': ..., 'partitionsOptimized': 1, 'numBatches': 2, 'totalConsideredFiles': 2, 'totalFilesSkipped': 0, 'preserveInsertionOrder': True}
            ```
        """
        cprint('Optimizing Delta Lake...', style=TextStyle.BOLD + RichColor.YELLOW)
        delta_table = self.get_delta_table()
        
        # Run optimization
        result = delta_table.optimize.compact(
            partition_filters=partition_filters,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            max_spill_size=max_spill_size,
            max_temp_directory_size=max_temp_directory_size,
            min_commit_interval=min_commit_interval,
            writer_properties=writer_properties,
            post_commithook_properties=post_commithook_properties,
            commit_properties=commit_properties,
        )
        
        cprint(f"Table: {delta_table.table_uri}", style=TextStyle.BOLD + RichColor.GREEN)
    
        # Extract the most important metrics
        files_added = result.get('numFilesAdded', 0)
        files_removed = result.get('numFilesRemoved', 0)
        partitions_optimized = result.get('partitionsOptimized', 0)
        
        # Create a summary panel
        cprint(Panel(
            f"Files added: {files_added}\n"
            f"Files removed: {files_removed}\n"
            f"Partitions optimized: {partitions_optimized}",
            title="Optimization Summary",
            border_style=RichColor.GREEN
        ))
        
        # Show detailed metrics in a collapsible panel if requested
        pretty_output = Pretty(result, expand_all=True)
        cprint(Panel(
            pretty_output,
            title="Detailed Metrics",
            border_style=RichColor.BLUE
        ))
            
        return result
