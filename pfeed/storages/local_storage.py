import pyarrow.fs as pa_fs

from pfeed.enums import DataStorage
from pfeed.storages.file_based_storage import FileBasedStorage


class LocalStorage(FileBasedStorage):
    name = DataStorage.LOCAL

    def get_filesystem(self) -> pa_fs.LocalFileSystem:
        return pa_fs.LocalFileSystem()
