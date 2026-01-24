from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed.typing import GenericData
    from pfeed.dataflow.sink import Sink


class DataFlowResult:
    def __init__(self):
        '''
        DataFlowResult is a class that contains the result of a flow.
        Attributes:
            _data: The output data of the flow.
            _metadata: Additional information about the flow.
                e.g. missing_dates, etc.
        '''
        self._data: GenericData | None = None
        self._metadata: dict[str, Any] = {}
        self._sink: Sink | None = None
        self._success: bool = False
    
    @property
    def data(self) -> GenericData | None:
        """
        Lazy-loads data from storage on first access.

        Data is only read from disk when this property is accessed, not when the
        DataFlowResult is created. This prevents large datasets from being held in
        memory unnecessarily, which is critical when using Ray for parallel processing:
        """
        if self._data is None:
            self._data = self._sink.storage.read_data()
        return self._data
    
    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata
    
    @property
    def success(self) -> bool:
        return self._success
    
    def set_success(self, success: bool):
        self._success = success
        
    def set_sink(self, sink: Sink):
        self._sink = sink
    
    def set_metadata(self, metadata: dict[str, Any]):
        self._metadata = metadata
    
    def add_metadata(self, key: str, value: Any) -> None:
        self._metadata[key] = value