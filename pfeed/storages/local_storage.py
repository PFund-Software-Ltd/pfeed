from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.enums import DataLayer
    from pathlib import Path

import pyarrow.fs as pa_fs

from pfeed.storages.file_based_storage import FileBasedStorage
from pfeed.enums import DataLayer
from pfeed.config import get_config


config = get_config()


class LocalStorage(FileBasedStorage):
    def __init__(
        self,
        data_path: Path | str | None = None,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str | Literal['MARKET_DATA', 'NEWS_DATA'] = 'MARKET_DATA',
        storage_options: dict | None = None,
    ):
        super().__init__(
            data_path=data_path or config.data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )
    
    def get_filesystem(self) -> pa_fs.LocalFileSystem:
        return pa_fs.LocalFileSystem()
