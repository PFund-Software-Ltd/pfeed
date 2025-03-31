from __future__ import annotations
from typing import Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.typing import GenericData


class Sink:
    def __init__(
        self, 
        data_model: BaseDataModel,
        create_storage_func: Callable,
    ):
        self.data_model = data_model
        self.data_source: BaseSource = data_model.data_source
        self._storage: BaseStorage | None = None
        self._create_storage_func = create_storage_func
    
    @property
    def storage(self) -> BaseStorage:
        if self._storage is None:
            self._storage = self._create_storage_func(self.data_model)
        return self._storage
    
    def __str__(self):
        if self._storage is None:
            return 'Sink(no storage)'
        else:
            return f'{self.storage.name} (data_layer={self.storage.data_layer.name.lower()}/data_domain={self.storage.data_domain})'

    def flush(self, data: GenericData):
        return self.storage.write_data(data)