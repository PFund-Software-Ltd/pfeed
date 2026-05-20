from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Any

if TYPE_CHECKING:
    from pfeed._io.database_io import DatabaseIO
    from pfeed._io.database_io import DBConnection, DBPath
    DatabaseURI: TypeAlias = str

from abc import ABC, abstractmethod

from pfeed.storages.base_storage import BaseStorage
from pfeed._io.io_config import IOConfig
from pfeed._sinks.sink_config import SinkConfig
from pfeed.enums import DataLayer


class DatabaseStorage(BaseStorage, ABC):
    _io: DatabaseIO

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
    def io(self) -> DatabaseIO:
        if not self._io:
            _ = self.with_io(io_config=IOConfig())
        return self._io

    @property
    def conn(self) -> DBConnection | None:
        db_path = self._get_db_path()
        return self.io.connect(db_path.db_uri)

    def with_io(self, io_config: IOConfig) -> BaseStorage:
        # database storage should only support one IO format
        if not self._has_only_one_io_format():
            raise NotImplementedError(f"Unhandled case: Multiple IO formats in {self.__class__.__name__}")
        return super().with_io(io_config=io_config)

    def with_sink(self, sink_config: SinkConfig) -> BaseStorage:
        # database storage should only support one sink
        if not self._has_only_one_sink():
            raise NotImplementedError(f"Unhandled case: Multiple sinks in {self.__class__.__name__}")
        return super().with_sink(sink_config=sink_config)
