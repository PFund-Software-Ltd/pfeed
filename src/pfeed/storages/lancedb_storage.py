from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from lancedb import LanceDBConnection

    from pfeed._io.lancedb_io import LanceDBIO

from pfeed.enums import IOFormat
from pfeed.storages.file_backed_database_storage import FileBackedDatabaseStorage


class LanceDBStorage(FileBackedDatabaseStorage):
    SUPPORTED_IO_FORMATS: ClassVar[list[Literal[IOFormat.LANCEDB]]] = [IOFormat.LANCEDB]

    _io: LanceDBIO
    conn: LanceDBConnection | None

    def _create_uri(self) -> str:
        return ""
