from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.sources.bybit.batch_api import BatchAPI
    from pfeed.sources.bybit.stream_api import StreamAPI

from pfeed.sources.bybit.source import BybitSource


class BybitMixin:
    data_source: BybitSource

    @property
    def batch_api(self) -> BatchAPI:
        return self.data_source.batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        return self.data_source.stream_api
    
    @staticmethod
    def _create_data_source(*args, **kwargs) -> BybitSource:
        return BybitSource()