from pathlib import Path
from pprint import pprint

from deltalake import DeltaTable

from pfund import cprint


class DeltaLakeStorageMixin:
    def get_delta_tables(self) -> list[DeltaTable]:
        if not self.use_deltalake:
            raise ValueError(f'{self.use_deltalake=} for this storage')
        from pfeed.storages.minio_storage import MinioStorage
        delta_tables = []
        storage_options = self.get_storage_options()
        if isinstance(self, MinioStorage):
            for obj in self.minio.list_objects(self.BUCKET_NAME, recursive=True):
                file_path = obj._object_name
                if '_delta_log' in file_path:
                    continue
                file_path_without_filename, filename = file_path.rsplit('/', 1)
                file_path_without_filename = 's3://' + self.BUCKET_NAME + '/' + file_path_without_filename
                if DeltaTable.is_deltatable(file_path_without_filename, storage_options=storage_options):
                    dt = DeltaTable(
                        file_path_without_filename,
                        storage_options=storage_options,
                    )
                    if dt.table_uri not in [_dt.table_uri for _dt in delta_tables]:
                        delta_tables.append(dt)
        # TODO: Azure, GCS, etc.
        else:
            data_path, _, _ = str(self.data_path).rsplit('/', 2)
            for file_dir_or_path in Path(data_path).rglob("*"):
                if 'minio' in str(file_dir_or_path) or file_dir_or_path.is_dir() or '_delta_log' in str(file_dir_or_path):
                    continue
                file_path_without_filename, filename = str(file_dir_or_path).rsplit('/', 1)
                if DeltaTable.is_deltatable(file_path_without_filename, storage_options=storage_options):
                    dt = DeltaTable(
                        file_path_without_filename,
                        storage_options=storage_options,
                    )
                    if dt.table_uri not in [_dt.table_uri for _dt in delta_tables]:
                        delta_tables.append(dt)
        return delta_tables
    
    def vacuum_delta_files(self, dry_run: bool=True):
        print('Vacuuming Delta Lake...')
        delta_tables = self.get_delta_tables()
        for dt in delta_tables:
            files_deleted = dt.vacuum(dry_run=dry_run)
            verb = 'Going to delete' if dry_run else 'Deleted'
            style = 'bold red' if dry_run else 'bold green'
            if files_deleted:
                cprint(f'{verb} {len(files_deleted)} files from {dt.table_uri}', style=style)
            else:
                cprint(f'No files to delete from {dt.table_uri}', style='bold blue')
    
    def optimize_delta_files(self):
        print('Optimizing Delta Lake...')
        delta_tables = self.get_delta_tables()
        for dt in delta_tables:
            output = dt.optimize.compact()
            print(dt.table_uri)
            pprint(output)
            print('-' * 100)
