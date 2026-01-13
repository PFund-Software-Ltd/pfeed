from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.storages.base_storage import BaseStorage
    from deltalake.table import FilterConjunctionType

from datetime import timedelta

from rich.pretty import Pretty
from rich.panel import Panel
from deltalake import DeltaTable

from pfund import cprint


class DeltaLakeStorageMixin:
    
    def get_delta_tables(self: BaseStorage | DeltaLakeStorageMixin) -> list[DeltaTable]:
        if not self.use_deltalake:
            raise ValueError(f'{self.use_deltalake=} for this storage')
        delta_tables = []
        storage_options = self._storage_options
        # if isinstance(self, MinioStorage):
        #     for obj in self.minio.list_objects(self.BUCKET_NAME, recursive=True):
        #         file_path = obj._object_name
        #         if '_delta_log' in file_path:
        #             continue
        #         file_path_without_filename, filename = file_path.rsplit('/', 1)
        #         file_path_without_filename = 's3://' + self.BUCKET_NAME + '/' + file_path_without_filename
        #         if DeltaTable.is_deltatable(file_path_without_filename, storage_options=storage_options):
        #             dt = DeltaTable(
        #                 file_path_without_filename,
        #                 storage_options=storage_options,
        #             )
        #             if dt.table_uri not in [_dt.table_uri for _dt in delta_tables]:
        #                 delta_tables.append(dt)
        for file_dir_or_path in self.data_path.parents[1].rglob("*"):
            if file_dir_or_path.is_dir() or '_delta_log' in str(file_dir_or_path):
                continue
            file_path_without_filename = file_dir_or_path.parent
            if DeltaTable.is_deltatable(str(file_path_without_filename), storage_options=storage_options):
                dt = DeltaTable(
                    str(file_path_without_filename),
                    storage_options=storage_options,
                )
                if dt.table_uri not in [_dt.table_uri for _dt in delta_tables]:
                    delta_tables.append(dt)
        return delta_tables
    
    def vacuum_delta_files(
        self: BaseStorage | DeltaLakeStorageMixin, 
        dry_run: bool=True, 
        retention_hours: int | None=None,
        enforce_retention_duration: bool=True,
        **deltalake_kwargs,
    ) -> list[str]:
        print('Vacuuming Delta Lake...')
        delta_tables = self.get_delta_tables()
        total_files_deleted = []
        for dt in delta_tables:
            files_deleted = dt.vacuum(
                dry_run=dry_run, 
                retention_hours=retention_hours,
                enforce_retention_duration=enforce_retention_duration,
                **deltalake_kwargs,
            )
            verb = 'Going to delete' if dry_run else 'Deleted'
            style = 'bold red' if dry_run else 'bold green'
            if files_deleted:
                total_files_deleted.extend(files_deleted)
                # Print the table URI with appropriate style
                cprint(f"{verb} {len(files_deleted)} files from {dt.table_uri}:", style=style)
                
                # Create a pretty-formatted version of the files list
                pretty_files = Pretty(files_deleted, expand_all=True)
                
                # Display the files in a panel with a border
                cprint(Panel(
                    pretty_files,
                    border_style=style.replace('bold ', ''),
                    title="Files to be deleted" if dry_run else "Deleted files",
                    title_align="left"
                ))
            else:
                cprint(f'No files to delete from {dt.table_uri}', style='bold blue')
        return total_files_deleted
    
    def optimize_delta_files(
        self: BaseStorage | DeltaLakeStorageMixin,
        partition_filters: FilterConjunctionType | None = None,
        target_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        min_commit_interval: int | timedelta | None = None,
        print_output: bool=True,
        **deltalake_kwargs,
    ):
        """
        Optimizes Delta tables by compacting small files into larger ones.
        
        Args:
            partition_filters: The partition filters that will be used for targeting specific partitions
                e.g. partition_filters=[('year', '=', 2023), ('month', '=', 1)]
            target_size: Desired file size after compaction, in bytes (default: 256MB if not set)
            max_concurrent_tasks: Maximum number of concurrent tasks (default: number of CPUs)
            min_commit_interval: Minimum interval before creating a new commit, as seconds or timedelta
                                (useful for long-running operations on large tables)
            print_output: Whether to print the output of the optimization
            **deltalake_kwargs: Additional arguments to pass to the Delta Lake compact function
        
        Returns:
            List of optimization results for each table
        """
        print('Optimizing Delta Lake...')
        delta_tables = self.get_delta_tables()
        results = []
        
        for dt in delta_tables:
            # Run optimization
            output = dt.optimize.compact(
                partition_filters=partition_filters,
                target_size=target_size,
                max_concurrent_tasks=max_concurrent_tasks,
                min_commit_interval=min_commit_interval,
                **deltalake_kwargs,
            )
            results.append(output)
            
            if print_output:
                cprint(f"Table: {dt.table_uri}")
            
                # Extract the most important metrics
                files_added = output.get('numFilesAdded', 0)
                files_removed = output.get('numFilesRemoved', 0)
                partitions_optimized = output.get('partitionsOptimized', 0)
                
                # Create a summary panel
                cprint(Panel(
                    f"Files added: {files_added}\n"
                    f"Files removed: {files_removed}\n"
                    f"Partitions optimized: {partitions_optimized}",
                    title="Optimization Summary",
                    border_style="green"
                ))
                
                # Show detailed metrics in a collapsible panel if requested
                pretty_output = Pretty(output, expand_all=True)
                cprint(Panel(
                    pretty_output,
                    title="Detailed Metrics",
                    border_style="blue"
                ))
            
        return results
