"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS, tSUPPORTED_DATA_TYPES
    from pfund.datas.resolution import Resolution
    from pfeed.config_handler import ConfigHandler
    
from pfeed.feeds.base_feed import BaseFeed


__all__ = ['BybitFeed']


class BybitFeed(BaseFeed):
    def __init__(self, data_tool: tSUPPORTED_DATA_TOOLS='pandas', config: ConfigHandler | None=None):
        super().__init__('bybit', data_tool=data_tool, config=config)
    
    def _get_data_from_source(
        self, 
        pdt: str, 
        date: str, 
        dtype: tSUPPORTED_DATA_TYPES, 
        resolution: Resolution,
    ) -> bytes | None:
        from pfeed import etl
        
        efilenames = self.api.get_efilenames(pdt)
        data_str = f'{self.name} {pdt} {date} {dtype} data'
        efilename = self._const.create_efilename(pdt, date)
        if efilename not in efilenames:
            self.logger.info(f'{data_str} is not found in data source')
            return None

        self.logger.warning(f"Downloading {data_str} on the fly, please consider using pfeed's {self.name}.download(...) to pre-download data to your local computer first")
        if raw_data := self.api.get_data(pdt, date):
            raw_tick: bytes = etl.clean_raw_data(self.name, raw_data)
            if dtype == 'raw_tick':
                data = raw_tick
            else:
                tick_data: bytes = etl.clean_raw_tick_data(raw_tick)
                if dtype == 'tick':
                    data = tick_data
                else:
                    data: bytes = etl.resample_data(tick_data, resolution)
                    self.logger.info(f'resampled {data_str} to {resolution=}')
            return data
        else:
            raise Exception(f'failed to download {data_str}, please check your network connection')
         
    # TODO?: maybe useful if used as a standalone program, not useful at all if used with PFund
    def get_realtime_data(self, env='LIVE'):
        pass
