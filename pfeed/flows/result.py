from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed.typing import GenericData


class FlowResult:
    def __init__(self):
        '''
        FlowResult is a class that contains the result of a flow.
        Attributes:
            _data: The output data of the flow.
            _metadata: Additional information about the flow.
                e.g. missing_dates, etc.
        '''
        self._data: GenericData | None = None
        self._metadata: dict[str, Any] = {}
    
    @property
    def data(self) -> GenericData | None:
        return self._data
    
    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata
        
    def set_data(self, data: GenericData | None):
        self._data = data
    
    def set_metadata(self, metadata: dict[str, Any]):
        self._metadata = metadata
    
    def add_metadata(self, key: str, value: Any) -> None:
        self._metadata[key] = value