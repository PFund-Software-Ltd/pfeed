"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS
    from pfeed.sources.bybit.types import tSUPPORTED_DATA_TYPES
    from pfund.datas.resolution import Resolution
    
from pfeed import etl
from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.bybit.utils import create_efilename


__all__ = ['BybitFeed']


class BybitFeed(BaseFeed):
    def __init__(self, data_tool: tSUPPORTED_DATA_TOOLS='pandas'):
        super().__init__('bybit', data_tool=data_tool)
    
    def _get_data_from_source(
        self, 
        pdt: str, 
        date: str, 
        dtype: tSUPPORTED_DATA_TYPES, 
        resolution: Resolution,
    ) -> bytes | None:        
        efilenames = self.api.get_efilenames(pdt)
        data_str = f'{self.name} {pdt} {date} {dtype} data'
        efilename = create_efilename(pdt, date)
        if efilename not in efilenames:
            self.logger.info(f'{data_str} is not found in data source')
            return None

        self.logger.warning(f"Downloading {data_str} on the fly, please consider using pe.download(data_source={self.name}, ...) to pre-download data to your local computer first")
        if raw_data := self.api.get_data(pdt, date):
            raw_tick: bytes = etl.clean_raw_data(self.name, raw_data)
            return self._transform_raw_data_to_dtype(raw_tick, dtype, resolution)
        else:
            raise Exception(f'failed to download {data_str}, please check your network connection')
    
    def _transform_raw_data_to_dtype(self, raw_tick: bytes, dtype: tSUPPORTED_DATA_TYPES, resolution: Resolution) -> bytes:
        if dtype == 'raw_tick':
            return raw_tick
        elif dtype == 'tick':
            return etl.clean_raw_tick_data(raw_tick)
        else:
            tick_data: bytes = etl.clean_raw_tick_data(raw_tick)
            data: bytes = etl.resample_data(tick_data, resolution)
            self.logger.info(f'resampled {self.name} raw tick  data to {resolution=}')
            return data

    # TODO?: maybe useful if used as a standalone program, not useful at all if used with PFund
    def get_realtime_data(self, env='LIVE'):
        pass
