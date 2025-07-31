from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.messaging.streaming_message import StreamingMessage
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.typing import GenericData

import logging


class Sink:
    def __init__(
        self, 
        data_model: BaseDataModel,
        storage: BaseStorage,
    ):
        self._data_model = data_model
        self._storage: BaseStorage = storage
        self._logger: logging.Logger = logging.getLogger(f"{self.data_source.name.lower()}_data")
    
    @property
    def data_source(self) -> BaseSource:
        return self._data_model.data_source
    
    @property
    def data_model(self) -> BaseDataModel | None:
        return self._data_model
    
    @property
    def storage(self) -> BaseStorage:
        return self._storage
    
    def __str__(self):
        if self._storage is None:
            return 'Sink(no storage)'
        else:
            return f'{self.storage.name} (data_layer={self.storage.data_layer.name.lower()}/data_domain={self.storage.data_domain})'

    def flush(self, data: GenericData | StreamingMessage, streaming: bool=False):
        try:
            log_level = logging.DEBUG if streaming else logging.INFO
            self.storage.write_data(data, streaming=streaming)
            self._logger.log(log_level, f'loaded {self.data_model} data to {self}')
        except Exception:
            self._logger.exception(f'failed to load {self.data_model} data (type={type(data)}) to {self}:')
