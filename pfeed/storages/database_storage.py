from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias
if TYPE_CHECKING:
    from pfeed._io.database_io import DBConnection
    DatabaseURI: TypeAlias = str

from abc import abstractmethod

from pfeed.storages.base_storage import BaseStorage


class DatabaseStorage(BaseStorage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # set data_path to be the URI of the database
        self.data_path: DatabaseURI = self._create_uri()

    @abstractmethod
    def _create_uri(self) -> DatabaseURI:
        """Create server connection URI without database name."""
        pass

    @property
    def conn(self) -> DBConnection:
        return self.io._conn
    
    def __enter__(self):
        return self  # Setup - returns the object to be used in 'with'
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()  # Cleanup - always runs at end of 'with' block
    
    def __del__(self):
        """Ensure connection is closed when object is garbage collected"""
        self.io._close_connection()
