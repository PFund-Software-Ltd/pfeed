from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Self
if TYPE_CHECKING:
    from pfeed._io.database_io import DBConnection, DBPath
    from pfeed.storages.storage_config import StorageConfig
    DatabaseURI: TypeAlias = str

from abc import abstractmethod

from pfeed.storages.base_storage import BaseStorage
from pfeed.enums import DataLayer


class DatabaseStorage(BaseStorage):
    def __init__(
        self,
        data_path: str | None,
        data_layer: DataLayer,
        data_domain: str,
        storage_options: dict | None = None,
    ):
        super().__init__(
            data_path=data_path or self._create_uri(),
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )

    @abstractmethod
    def _create_uri(self) -> DatabaseURI:
        """Create server connection URI without database name."""
        pass

    @classmethod
    def from_storage_config(cls, storage_config: StorageConfig) -> Self:
        return cls(
            data_path=storage_config.data_path,
            data_layer=storage_config.data_layer,
            data_domain=storage_config.data_domain,
            data_storage=storage_config.storage,
            storage_options=storage_config.storage_options,
        )

    def _get_db_path(self) -> DBPath:
        return self.data_handler._db_path

    @property
    def conn(self) -> DBConnection | None:
        if self.io:
            db_path = self._get_db_path()
            self.io.connect(db_path.db_uri)
            return self.io._conn
        else:
            return None
