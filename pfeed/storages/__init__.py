from __future__ import annotations
from typing import TYPE_CHECKING, overload, Literal
if TYPE_CHECKING:
    from pfeed._typing import tDataLayer, tStorage, tStreamMode
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.storages.minio_storage import MinioStorage


@overload
def create_storage(self,
    storage: Literal['DUCKDB'],
    data_model: BaseDataModel,
    data_layer: tDataLayer,
    data_domain: str,
    use_deltalake: bool=False,
    storage_options: dict | None=None,
    stream_mode: tStreamMode='FAST',
    delta_flush_interval: int=100,
) -> DuckDBStorage:
    ...
    
    
@overload
def create_storage(self,
    storage: Literal['MINIO'],
    data_model: BaseDataModel,
    data_layer: tDataLayer,
    data_domain: str,
    use_deltalake: bool=False,
    storage_options: dict | None=None,
    stream_mode: tStreamMode='FAST',
    delta_flush_interval: int=100,
) -> MinioStorage:
    ...


def create_storage(
    storage: tStorage,
    data_model: BaseDataModel,
    data_layer: tDataLayer,
    data_domain: str,
    use_deltalake: bool=False,
    storage_options: dict | None=None,
    stream_mode: tStreamMode='FAST',
    delta_flush_interval: int=100,
) -> BaseStorage:
    from pfeed.enums import DataStorage, StreamMode
    Storage = DataStorage[storage.upper()].storage_class
    return Storage.from_data_model(
        data_model=data_model,
        data_layer=data_layer,
        data_domain=data_domain,
        use_deltalake=use_deltalake,
        storage_options=storage_options,
        stream_mode=StreamMode[stream_mode.upper()],
        delta_flush_interval=delta_flush_interval,
    )