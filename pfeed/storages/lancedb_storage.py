from pfeed.enums import DataStorage
from pfeed.storages.database_storage import DatabaseStorage
from pfeed._io.lancedb_io import LanceDBIO


class LanceDBStorage(DatabaseStorage):
    name = DataStorage.LANCEDB
    
    # TODO: add __new__ to determine if inherit from LocalStorage or S3Storage?
    def __new__(cls, *args, **kwargs):
        pass
    
    def with_io(self, io_options: dict | None = None) -> LanceDBIO:
        return super().with_io(io_options=io_options)
    
    def _create_io(self, io_options: dict | None = None) -> LanceDBIO:
        return LanceDBIO(
            filesystem=self.get_filesystem(),
            storage_options=self.storage_options,
            io_options=io_options,
        )
