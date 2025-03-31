from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Any
if TYPE_CHECKING:
    from bytewax.dataflow import Stream as BytewaxStream
    from bytewax.inputs import Source as BytewaxSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.typing import GenericData
    
from pfeed.enums.extract_type import ExtractType


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    def __init__(
        self, 
        data_model: BaseDataModel, 
        extract_func: Callable,  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
    ):
        self.data_model = data_model
        self.data_source: BaseSource = data_model.data_source
        self.extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        self._extract_func = extract_func
        self._streaming = self.extract_type == ExtractType.stream

    def __str__(self):
        return f'{self.data_source.name}.{self.extract_type}'
    
    def open(self) -> BytewaxSource | BytewaxStream | tuple[GenericData | None, dict[str, Any]]:
        if self._streaming:
            # TODO: streaming without bytewax
            source: BytewaxSource | BytewaxStream = self._extract_func(self.data_model)
            return source
        else:
            if self.extract_type == ExtractType.retrieve:
                data, metadata = self._extract_func(self.data_model)
                if 'updated_resolution' in metadata:
                    self.data_model.update_resolution(metadata['updated_resolution'])
                    del metadata['updated_resolution']
            else:
                data: GenericData | None = self._extract_func(self.data_model)
                # NOTE: currently no metadata for other extract_types
                metadata = {}
            return data, metadata
    
    # TODO: useful for streaming, disconnect the stream?
    # def close(self):
    #     pass