from pfeed.storages.database_storage import DatabaseStorage
from pfeed.enums import IOFormat
from pfeed._io.lancedb_io import LanceDBIO


class LanceDBStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.LANCEDB]

    # TODO: add __new__ to determine if inherit from LocalStorage or S3Storage?
    def __new__(cls, *args, **kwargs):
        pass
    
    def with_io(self, io_options: dict | None = None, **kwargs) -> LanceDBIO:
        '''
        Args:
            kwargs: Unused parameters accepted for compatibility with other storage backends
        '''
        return super().with_io(io_options=io_options)
    
    def _create_io(self, io_options: dict | None = None) -> LanceDBIO:
        return LanceDBIO(
            filesystem=self.get_filesystem(),
            storage_options=self.storage_options,
            io_options=io_options,
        )
