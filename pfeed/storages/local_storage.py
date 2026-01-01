from pathlib import Path

import pyarrow.fs as pa_fs

from pfeed.typing import tDataLayer
from pfeed.enums import DataStorage
from pfeed.storages.base_storage import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(
        self,
        data_layer: tDataLayer,
        data_domain: str,
        name: DataStorage=DataStorage.LOCAL,
        base_data_path: Path | None = None,
        storage_options: dict | None=None,
    ):
        super().__init__(
            name=name,
            data_layer=data_layer,
            data_domain=data_domain,
            base_data_path=base_data_path,
            storage_options=storage_options,
        )

    def get_filesystem(self) -> pa_fs.LocalFileSystem:
        return pa_fs.LocalFileSystem()
