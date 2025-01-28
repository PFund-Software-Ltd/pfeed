from __future__ import annotations
from typing import Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from bytewax.dataflow import Dataflow as BytewaxDataFlow
    from bytewax.dataflow import Stream as BytewaxStream
    from bytewax.inputs import Source as BytewaxSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.sources.base_source import BaseSource
    from pfeed.typing.core import tData
    
from enum import StrEnum

from pfeed.utils.utils import lambda_with_name


class ExtractType(StrEnum):
    download = 'download'
    stream = 'stream'
    retrieve = 'retrieve'
    fetch = 'fetch'


class Faucet:
    '''Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    '''
    def __init__(
        self, 
        data_model: BaseDataModel, 
        execute_func: Callable,  # e.g. _execute_download(), _execute_stream(), _execute_retrieve(), _execute_fetch()
        name: str='',
        streaming: bool=False,
    ):
        self.data_model = data_model
        self.data_source: BaseSource = data_model.source
        self.op_type = ExtractType[self.execute_func.__name__.replace('_execute_', '')]
        self.name = self._create_name(execute_func, name)
        self.execute_func = lambda_with_name(self.name, execute_func)
        self._streaming = streaming

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
    
    def open(self) -> tData | None | BytewaxSource | BytewaxStream:
        if self._streaming:
            # TODO: streaming without bytewax
            source: BytewaxSource | BytewaxStream = self.execute_func()
            return source
        else:
            data: tData | None = self.execute_func()
            return data
    
    def __str__(self):
        return self.name

    # TODO: useful for streaming, disconnect the stream?
    # def close(self):
    #     pass