from __future__ import annotations
from typing import Callable, TYPE_CHECKING, Literal, Any
if TYPE_CHECKING:
    from bytewax.dataflow import Stream as BytewaxStream
    from bytewax.inputs import Source as BytewaxSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.typing.core import tData
    
from pfeed.enums.extract_type import ExtractType
from pfeed.utils.utils import lambda_with_name


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    def __init__(
        self, 
        data_model: BaseDataModel, 
        execute_func: Callable,  # e.g. _execute_download(), _execute_stream(), _execute_retrieve(), _execute_fetch()
        op_type: ExtractType | Literal['download', 'stream', 'retrieve', 'fetch'],
        name: str='',
    ):
        self.data_model = data_model
        self.data_source: BaseSource = data_model.data_source
        self.op_type = ExtractType[op_type.lower()] if isinstance(op_type, str) else op_type
        self.name = self._create_name(name)
        self.execute_func = lambda_with_name(self.name, execute_func)
        self._streaming = self.op_type == ExtractType.stream

    def _create_name(self, name: str):
        name_list = []
        source_name = self.data_source.name
        if source_name not in name and source_name.lower() not in name:
            name_list.append(source_name)
        if self.op_type not in name and self.op_type.upper() not in name:
            name_list.append(self.op_type)
        if name:
            name_list.append(name)
        return '.'.join(name_list)
    
    def open(self) -> BytewaxSource | BytewaxStream | tuple[tData | None, dict[str, Any]]:
        if self._streaming:
            # TODO: streaming without bytewax
            source: BytewaxSource | BytewaxStream = self.execute_func()
            return source
        else:
            if self.op_type == ExtractType.retrieve:
                data, metadata = self.execute_func()
            else:
                data: tData | None = self.execute_func()
                # currently no metadata for other op_types
                metadata = {}
            return data, metadata
    
    def __str__(self):
        return self.name

    # TODO: useful for streaming, disconnect the stream?
    # def close(self):
    #     pass