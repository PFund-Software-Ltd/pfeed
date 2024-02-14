import io
import datetime

import pandas as pd

from pfeed.config_handler import ConfigHandler
from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.bybit import api
from pfeed.sources.bybit import etl
from pfeed.sources.bybit.const import DATA_SOURCE, create_efilename
from pfeed.utils.utils import get_dates_in_between, rollback_date_range


__all__ = ['BybitFeed']


class BybitFeed(BaseFeed):
    def __init__(self, config: ConfigHandler | None=None):
        super().__init__('bybit', config=config)
    
    @staticmethod
    def _derive_dtype_from_resolution(resolution):
        from pfund.datas.resolution import Resolution
        resolution = Resolution(resolution)
        if resolution.is_tick():
            return 'tick'
        elif resolution.is_second():
            return 'second'
        elif resolution.is_minute():
            return 'minute'
        elif resolution.is_hour():
            return 'hour'
        elif resolution.is_day():
            return 'daily'
        else:
            raise Exception(f'{resolution=} is not supported')
    
    def get_historical_data(
        self,
        pdt: str,
        rollback_period: str='1w',
        resolution: str='1d',
        start_date: str=None,
        end_date: str=None,
    ) -> pd.DataFrame:
        from pfund.exchanges.bybit.exchange import Exchange
        
        source = DATA_SOURCE
        exchange = Exchange(env='LIVE')
        adapter = exchange.adapter
        dtype = self._derive_dtype_from_resolution(resolution)
        product = exchange.create_product(*pdt.split('_'))
        category = product.category
        epdt = adapter(pdt, ref_key=category)
        efilenames = api.get_efilenames(category, epdt)
        
        if start_date:
            # default for end_date is yesterday
            end_date: str = end_date or (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        
        dfs = []
        dates = [date for date in dates if create_efilename(epdt, date, is_spot=product.is_spot()) in efilenames]
        for date in dates:
            data_str = f'{source} {pdt} {date}'
            if local_data := etl.extract_data(pdt, date, dtype, mode='historical', data_path=self.data_path):
                # e.g. local_data could be 1m data (period always = 1), but resampled_data could be 3m data
                resampled_data: bytes = etl.resample_data(local_data, resolution, is_tick=(dtype == 'tick'), category=category)
                print(f'loaded {data_str} local {dtype} data')
            else:
                print(f'Downloading {data_str} data on the fly, please consider using {source.lower()}.run_historical(...) to pre-download data to your local computer first')
                if raw_data := api.get_data(category, epdt, date):
                    tick_data: bytes = etl.clean_data(category, raw_data)
                    resampled_data: bytes = etl.resample_data(tick_data, resolution, is_tick=True, category=category)
                    print(f'resampled {data_str} data to {resolution=}')
                else:
                    raise Exception(f'failed to download {data_str} historical data')
            df = pd.read_parquet(io.BytesIO(resampled_data))
            dfs.append(df)
        return pd.concat(dfs)
    
    # TODO?: maybe useful if used as a standalone program, not useful at all if used with PFund
    def get_real_time_data(self, env='LIVE'):
        pass
    
        
if __name__ == '__main__':
    feed = BybitFeed()
    df = feed.get_historical_data('BCH_USDT_PERP', resolution='1d', rollback_period='2d')
    print(df)