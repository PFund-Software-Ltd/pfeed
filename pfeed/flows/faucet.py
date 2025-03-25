from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal, Any
if TYPE_CHECKING:
    from bytewax.dataflow import Stream as BytewaxStream
    from bytewax.inputs import Source as BytewaxSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.typing import GenericData
    
from pfeed.enums.extract_type import ExtractType
from pfeed.utils.utils import lambda_with_name


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    def __init__(
        self, 
        data_model: BaseDataModel, 
        extract_func: Callable,  # e.g. _download_impl(), _stream_impl(), _retrieve_impl(), _fetch_impl()
        extract_type: ExtractType,
        name: str='',
    ):
        self.data_model = data_model
        self.data_source: BaseSource = data_model.data_source
        self.extract_type = ExtractType[extract_type.lower()] if isinstance(extract_type, str) else extract_type
        self.name = self._create_name(name)
        self.extract_func = lambda_with_name(self.name, extract_func)
        self._streaming = self.extract_type == ExtractType.stream

    def _create_name(self, name: str):
        name_list = []
        source_name = self.data_source.name
        if source_name not in name and source_name.lower() not in name:
            name_list.append(source_name)
        if self.extract_type not in name and self.extract_type.upper() not in name:
            name_list.append(self.extract_type)
        if name:
            name_list.append(name)
        return '.'.join(name_list)
    
    def open(self) -> BytewaxSource | BytewaxStream | tuple[GenericData | None, dict[str, Any]]:
        if self._streaming:
            # TODO: streaming without bytewax
            source: BytewaxSource | BytewaxStream = self.extract_func(self.data_model)
            return source
        else:
            if self.extract_type == ExtractType.retrieve:
                data, metadata = self.extract_func(self.data_model)
            else:
                data: GenericData | None = self.extract_func(self.data_model)
                # currently no metadata for other extract_types
                metadata = {}
            return data, metadata
    
    def __str__(self):
        return self.name

    # TODO: useful for streaming, disconnect the stream?
    # def close(self):
    #     pass