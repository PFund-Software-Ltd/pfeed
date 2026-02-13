from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable
if TYPE_CHECKING:
    from pfeed.typing import GenericData
    from pfeed.data_handlers.base_data_handler import BaseMetadata


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
        self._metadata: BaseMetadata | None = None
        self._data_loader: Callable | None = None  # Function called to lazy-load data when accessed
        self._success: bool = False
    
    @property
    def data(self) -> GenericData | None:
        """
        Lazy-loads data from storage on first access.

        Data is only read from disk when this property is accessed, not when the
        DataFlowResult is created. This prevents large datasets from being held in
        memory unnecessarily, which is critical when using Ray for parallel processing:
        """
        if self._data is None and self._data_loader:
            self._data = self._data_loader()
        return self._data
    
    @property
    def metadata(self) -> BaseMetadata | None:
        return self._metadata
    
    @property
    def success(self) -> bool:
        return self._success
    
    def set_success(self, success: bool):
        self._success = success
    
    def set_data(self, data: GenericData):
        self._data = data
    
    def set_data_loader(self, func: Callable):
        self._data_loader = func
        
    def set_metadata(self, metadata: BaseMetadata):
        self._metadata = metadata
