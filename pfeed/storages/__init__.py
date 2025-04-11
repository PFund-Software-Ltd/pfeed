from __future__ import annotations
from typing import TYPE_CHECKING, overload, Literal
if TYPE_CHECKING:
    from pfeed.typing import tDATA_LAYER, tSTORAGE
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.storages.duckdb_storage import DuckDBStorage
    from pfeed.storages.minio_storage import MinioStorage


@overload
def create_storage(self,
    storage: Literal['duckdb'],
    data_model: BaseDataModel,
    data_layer: tDATA_LAYER,
    data_domain: str,
    use_deltalake: bool=False,
    storage_options: dict | None=None,
) -> DuckDBStorage:
    ...
    
    
@overload
def create_storage(self,
    storage: Literal['minio'],
    data_model: BaseDataModel,
    data_layer: tDATA_LAYER,
    data_domain: str,
    use_deltalake: bool=False,
    storage_options: dict | None=None,
) -> MinioStorage:
    ...


@staticmethod
def create_storage(
    storage: tSTORAGE,
    data_model: BaseDataModel,
    data_layer: tDATA_LAYER,
    data_domain: str,
    use_deltalake: bool=False,
    storage_options: dict | None=None,
) -> BaseStorage:
    from pfeed.enums import DataStorage
    storage_options = storage_options or {}
    Storage = DataStorage[storage.upper()].storage_class
    return Storage.from_data_model(
        data_model=data_model,
        data_layer=data_layer,
        data_domain=data_domain,
        use_deltalake=use_deltalake,
        storage_options=storage_options,
    )