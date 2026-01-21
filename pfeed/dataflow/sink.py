from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.messaging.streaming_message import StreamingMessage
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.typing import GenericData

import logging


class Sink:
    def __init__(self, data_model: BaseDataModel, storage: BaseStorage):
        self._data_model = data_model
        self._storage: BaseStorage = storage
        self._logger: logging.Logger = logging.getLogger(f'pfeed.{self.data_source.name.lower()}')
    
    @property
    def data_source(self) -> DataProviderSource:
        return self._data_model.data_source
    
    @property
    def data_model(self) -> BaseDataModel | None:
        return self._data_model
    
    @property
    def storage(self) -> BaseStorage:
        return self._storage
    
    def __str__(self):
        if self._storage is None:
            return 'Sink (no storage)'
        else:
            return f'Sink (storage={self.storage})'

    # OPTIMIZE: use a thread pool for data writing (I/O bound)? not sure how much IO bound is writing to a delta lake
    def flush(self, data: GenericData | StreamingMessage, streaming: bool=False):
        try:
            log_level = logging.DEBUG if streaming else logging.INFO
            self.storage.write_data(data, streaming=streaming)
            self._logger.log(log_level, f'loaded {self.data_model} data to {self.storage}')
        except Exception:
            self._logger.exception(f'failed to load {self.data_model} data to {self.storage}:')
