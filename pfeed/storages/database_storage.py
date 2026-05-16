from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Any

if TYPE_CHECKING:
    from pfeed._io.database_io import DatabaseIO
    from pfeed._io.database_io import DBConnection, DBPath
    DatabaseURI: TypeAlias = str

from abc import ABC, abstractmethod

from pfeed.storages.base_storage import BaseStorage
from pfeed.enums import DataLayer


class DatabaseStorage(BaseStorage, ABC):
    io: DatabaseIO

    def __init__(
        self,
        data_path: str | None,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
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

    def _get_db_path(self) -> DBPath:
        assert self.data_handler._db_path is not None
        return self.data_handler._db_path

    @property
    def conn(self) -> DBConnection | None:
        if self.io:
            db_path = self._get_db_path()
            return self.io.connect(db_path.db_uri)
        else:
            return None
