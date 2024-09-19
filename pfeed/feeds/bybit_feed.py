"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS
    from pfeed.resolution import ExtendedResolution


from pfeed.feeds.base_feed import BaseFeed


__all__ = ['BybitFeed']


class BybitFeed(BaseFeed):
    def __init__(self, data_tool: tSUPPORTED_DATA_TOOLS='pandas'):
        super().__init__('bybit', data_tool=data_tool)
    
    def _get_historical_data_from_source(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, dates: list[datetime.date]) -> list[bytes]:
        from pfeed import etl
        from pfeed.sources.bybit.utils import create_efilename
        
        efilenames = self.api.get_efilenames(pdt)
        raw_resolution = self.utils.get_default_raw_resolution()
        
        datas: list[bytes] = []
        for date in dates:
            data_str = f'{self.name} {pdt} {date} {resolution} data'
            efilename = create_efilename(pdt, date)
            if efilename not in efilenames:
                self.logger.info(f'{data_str} is not found in data source')
                continue
            self.logger.warning(f"Downloading {data_str} on the fly, please consider using pe.download(data_source={self.name}, ...) to pre-download data to your local computer first")
            if raw_data := self.api.get_data(pdt, date):
                data: bytes = etl.clean_raw_data(self.name, raw_data)
                data: bytes = etl.transform_data(
                    self.name,
                    pdt,
                    data,
                    raw_resolution,
                    resolution,
                )
                datas.append(data)
            else:
                raise Exception(f'failed to download {data_str}, please check your network connection')
        return datas
    
    # TODO?: maybe useful if used as a standalone program, not useful at all if used with PFund
    def get_realtime_data(self, env='LIVE'):
        pass
