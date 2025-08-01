from typing import Literal

import pyarrow.fs as pa_fs

from pfeed._typing import tDataLayer
from pfeed.storages.base_storage import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(
        self,
        name: Literal['LOCAL', 'CACHE']='LOCAL',
        data_layer: tDataLayer='CLEANED',
        data_domain: str='general_data',
        use_deltalake: bool=False, 
        storage_options: dict | None=None,
    ):
        super().__init__(
            name=name, 
            data_layer=data_layer, 
            data_domain=data_domain, 
            use_deltalake=use_deltalake, 
            storage_options=storage_options,
        )

    def get_filesystem(self) -> pa_fs.LocalFileSystem:
        return pa_fs.LocalFileSystem()
