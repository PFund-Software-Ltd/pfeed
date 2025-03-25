from typing import Literal

import pyarrow.fs as pa_fs

from pfeed.typing import tDATA_LAYER
from pfeed.storages.base_storage import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(
        self,
        name: Literal['local', 'cache']='local',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        use_deltalake: bool=False, 
        **kwargs
    ):
        super().__init__(name=name, data_layer=data_layer, data_domain=data_domain, use_deltalake=use_deltalake, **kwargs)

    def get_filesystem(self) -> pa_fs.LocalFileSystem:
        return pa_fs.LocalFileSystem()
