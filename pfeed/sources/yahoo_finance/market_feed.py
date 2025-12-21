from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Awaitable
if TYPE_CHECKING:
    import pandas as pd
    from yfinance import Ticker
    from pfund.typing import FullDataChannel, tEnvironment
    from pfund.datas.resolution import Resolution
    from pfund.products.product_base import BaseProduct
    from pfeed.typing import tStorage, tDataLayer, GenericFrame, GenericFrameOrNone
    from pfeed.sources.yahoo_finance.stream_api import ChannelKey
    from pfeed.sources.yahoo_finance.market_data_model import YahooFinanceMarketDataModel

import time
import datetime

from pfeed.feeds.market_feed import MarketFeed
from pfeed.sources.yahoo_finance.mixin import YahooFinanceMixin


__all__ = ["YahooFinanceMarketFeed"]


# NOTE: only yfinance's period='max' is used, everything else is converted to start_date and end_date
# i.e. any resampling inside yfinance (interval always ='1x') is not used, it's all done by pfeed
class YahooFinanceMarketFeed(YahooFinanceMixin, MarketFeed):
    _URLS = {
        "rest": "https://query1.finance.yahoo.com",
        "ws": "wss://streamer.finance.yahoo.com",
    }
    _ADAPTER = {
        "timeframe": {
            # month
            "M": "mo",
            "mo": "M",
            # week
            "w": "wk",
            "wk": "w",
        }
    }
    # yfinance's valid periods = pfund's rollback_periods
    # SUPPORTED_ROLLBACK_PERIODS = {
    #     "d": [1, 5],
    #     "M": [1, 3, 6],
    #     "y": [1, 2, 5, 10],
    # }
    
    # yfinance's valid intervals = pfund's resolutions
    # SUPPORTED_RESOLUTIONS = {
    #     "m": [1, 2, 5, 15, 30, 60, 90],
    #     "h": [1],
    #     "d": [1, 5],
    #     "w": [1],
    #     "M": [1, 3],
    # }
    
    def create_data_model(
        self,
        env: tEnvironment,
        product: str | BaseProduct,
        resolution: str | Resolution,
        start_date: str | datetime.date,
        end_date: str | datetime.date | None = None,
        data_origin: str = '',
        **product_specs
    ) -> YahooFinanceMarketDataModel:
        from pfeed.sources.yahoo_finance.market_data_handler import YahooFinanceMarketDataHandler
        data_model = super().create_data_model(
            env=env,
            product=product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            **product_specs
        )
        data_model.data_handler_class = YahooFinanceMarketDataHandler
        return data_model

    @staticmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        # convert to UTC and reset index
        df.index = df.index.tz_convert("UTC").tz_localize(None)
        df.reset_index(inplace=True)
        
        # convert column names to lowercase and replace spaces with underscores
        # NOTE:
        # if there are spaces in column names, they will be turned into some weird names like "_10" during "for row in df.itertuples()"
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]

        # convert volume (int) to float
        df['volume'] = df['volume'].astype(float)
        
        # somehow different column names "Date" and "Datetime" are used in yfinance depending on the resolution
        RENAMING_COLS = {'datetime': 'date', 'stock_splits': 'splits'}
        df = df.rename(columns=RENAMING_COLS)
        return df
    
    def _handle_rollback_max_period(self, resolution: Resolution | str | Literal['minute', 'hour', 'day'], start_date: str, end_date: str):
        from pfeed.enums import MarketDataType
        resolution: Resolution = self._create_resolution(resolution)
        dtype = MarketDataType[str(resolution.timeframe)]
        if dtype == MarketDataType.DAY:
            # HACK: use '1900-01-01' as the start date for daily data since we don't know the exact start date when rollback_period == 'max'
            start_date = '1900-01-01'
            end_date = ''
            rollback_period = ''
        elif dtype == MarketDataType.HOUR:
            rollback_period = '2y'  # max is 2 years for hourly data
        elif dtype == MarketDataType.MINUTE:
            rollback_period = '8d'  # max is 8 days for minute data
        return start_date, end_date, rollback_period

    def _check_yfinance_kwargs(self, yfinance_kwargs: dict | None) -> dict:
        if self._yfinance_kwargs is not None:
            return self._yfinance_kwargs
        yfinance_kwargs = yfinance_kwargs or {}
        assert "interval" not in yfinance_kwargs, "`interval` duplicates with pfeed's `resolution`, please remove it"
        assert "period" not in yfinance_kwargs, "`period` duplicates with pfeed's `rollback_period`, please remove it"
        assert "start" not in yfinance_kwargs, "`start` duplicates with pfeed's `start_date`, please remove it"
        assert "end" not in yfinance_kwargs, "`end` duplicates with pfeed's `end_date`, please remove it"
        return yfinance_kwargs
    
    def download(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | str | Literal['minute', 'hour', 'day', 'max']='day',
        rollback_period: str | Literal["ytd", "max"]='max',
        start_date: str='',
        end_date: str='',
        data_layer: tDataLayer='CLEANED',
        data_origin: str='',
        to_storage: tStorage | None='LOCAL',
        storage_options: dict | None=None,
        auto_transform: bool=True,
        yfinance_kwargs: dict | None=None,
        **product_specs
    ) -> GenericFrameOrNone | YahooFinanceMarketFeed:
        '''
        Download historical data from Yahoo Finance.
        Be reminded that if you include today's data, it can be incomplete, this especially applies to the usage of rollback_period.
        e.g. rollback_period='ytd'/'max' includes today's data, and today is not finished, so the data is incomplete.
        start_date and end_date (not including today) should be specified to avoid this issue.
        
        Args:
            product_specs: The specifications for the product.
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
            rollback_period: Data resolution or 'ytd' or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1M' = 1 month.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use rollback_period to determine the start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            yfinance_kwargs: kwargs supported by `yfinance`
                refer to kwargs in history() in yfinance/scrapers/history.py
        '''
        self._yfinance_kwargs = self._check_yfinance_kwargs(yfinance_kwargs)
        if rollback_period == 'max' and not start_date:
            start_date, end_date, rollback_period = self._handle_rollback_max_period(resolution, start_date, end_date)
        return super().download(
            product=product,
            symbol=symbol,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_origin=data_origin,
            to_storage=to_storage,
            storage_options=storage_options,
            auto_transform=auto_transform,
            dataflow_per_date=False,
            include_metadata=False,
            **product_specs
        )
    
    def _download_impl(self, data_model: YahooFinanceMarketDataModel) -> pd.DataFrame | None:
        # convert pfund's resolution format to yfinance's interval
        resolution = data_model.resolution
        timeframe = repr(resolution.timeframe)
        etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)
        eresolution = str(resolution.period) + etimeframe
        
        product = data_model.product
        symbol = product.symbol
        assert symbol, f'symbol is required for {data_model}'
        ticker = self.batch_api.Ticker(symbol)
        
        no_df = True
        NUM_RETRIES = 3
        # NOTE: yfinance's end_date is not inclusive, so we need to add 1 day to the end_date
        yfinance_end_date = data_model.end_date + datetime.timedelta(days=1)
        
        while no_df and NUM_RETRIES:
            NUM_RETRIES -= 1
            self.logger.debug(f'downloading {data_model}')
            # NOTE: yfinance's period is not used, only use start_date and end_date for data clarity in storage
            df: pd.DataFrame | None = ticker.history(
                interval=eresolution,
                start=str(data_model.start_date),
                end=str(yfinance_end_date),
                **self._yfinance_kwargs
            )
            no_df = (df is None or df.empty)
            if no_df:
                self.logger.info(f'failed to download {product.symbol} {resolution} data, retrying...')
                time.sleep(1)
            else:
                self.logger.debug(f'downloaded {data_model}')
                if str(data_model.start_date) == '1900-01-01':  # rollback_period='max' for daily data
                    actual_start_date = min(df.index).date()
                    data_model.update_start_date(actual_start_date)
                    self.logger.debug(f'set start_date={actual_start_date} for {data_model}')
                break
        else:
            self.logger.warning(f'failed to download {product.symbol} {resolution} data, '
                                f'please check if start_date={data_model.start_date} and end_date={data_model.end_date} is within a valid range')
        # REVIEW: for some unknown reason, yfinance sometimes returns None or empty DataFrame even start_date and end_date are within a valid range
        # i.e. same function call, different results, need to remind users to manually retry again
        if df is None or df.empty:
            df_msg = 'df=None' if df is None else 'empty df'
            self.logger.warning(
                f'downloaded {df_msg} {product.symbol} {resolution} data from {self.name}. '
                'If it happens on trading days, it is possibly due to rate limit, network issue, or bugs in `pfeed` or `yfinance`, '
                'you may try to modify (extend the time range) `start_date` and `end_date` and avoid using `rollback_period` to try again.'
            )
        self._yfinance_kwargs.clear()
        return df
    
    async def _stream_impl(self, faucet_streaming_callback: Callable[[str, dict, YahooFinanceMarketDataModel | None], Awaitable[None] | None]):
        stream_api = self.data_source.stream_api
        async def _callback(msg: dict):
            symbol = msg['id']
            channel_key: ChannelKey = stream_api.generate_channel_key(symbol)
            if channel_key not in stream_api._last_day_volume:
                # needs two 'day_volume' and 'time' to derive the volume, so return for the first message
                stream_api._last_day_volume[channel_key] = int(msg['day_volume'])
                stream_api._last_time_in_mts[channel_key] = int(msg['time'])
                return
            data_model = stream_api._streaming_bindings[channel_key]
            await faucet_streaming_callback(self.name.value, msg, data_model)
        stream_api.set_callback(_callback)
        await stream_api.connect()
    
    async def _close_stream(self):
        await self.data_source.stream_api.disconnect()
    
    def _add_default_transformations_to_stream(self, product: BaseProduct, resolution: Resolution):
        from pfeed.utils import lambda_with_name
        # since Ray can't serialize the "self" in self._parse_message, disable it for now
        assert self._use_ray is False, "Transformations in Yahoo Finance streaming data is not supported with Ray, please set use_ray=False"
        self.transform(
            lambda_with_name('parse_message', lambda msg: self._parse_message(product, msg)),
        )
        super()._add_default_transformations_to_stream(product, resolution)
    
    def _parse_message(self, product: BaseProduct, msg: dict) -> dict:
        '''
        Args:
            msg: raw message from yahoo finance streaming data
        NOTE: lots of quirks in yahoo finance streaming data:
        - weird 'last_size', sometimes it's provided, sometimes not
        - 'time' can be duplicated, i.e. trades can be backfilled, 
        e.g.
            {'id': 'AAPL', 'price': 230.605, 'time': '1755531691000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 1, 'change_percent': -0.42533004, 'day_volume': '14805050', 'change': -0.9850006, 'last_size': '120', 'price_hint': '2'}
            {'id': 'AAPL', 'price': 230.605, 'time': '1755531691000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 1, 'change_percent': -0.4253209, 'day_volume': '14805114', 'change': -0.9850006, 'price_hint': '2'}
        currently interpret it as this is how yahoo finance handles delayed trades
        REVIEW: current solution is, ignore 'last_size', derive volume = diff(day_volume - previous day_volume); +1 to duplicated 'time'
        another more accurate solution to handle duplicated 'time' is, wait for the 'time' to change to get the most accurate volume, 
        but it might not be worth it due the quality of yahoo finance streaming data
        ''' 
        stream_api = self.data_source.stream_api
        channel_key: ChannelKey = stream_api.generate_channel_key(product.symbol)
        
        # compute traded volume
        last_day_volume = stream_api._last_day_volume[channel_key]
        current_day_volume = int(msg['day_volume'])
        volume = current_day_volume - last_day_volume
        stream_api._last_day_volume[channel_key] = current_day_volume
        
        # detect duplicated 'time', if duplicated, add 1 to it to make it unique
        last_time_in_mts = stream_api._last_time_in_mts[channel_key]
        current_time_in_mts = int(msg['time'])
        if current_time_in_mts == last_time_in_mts:
            current_time_in_mts += 1
        stream_api._last_time_in_mts[channel_key] = current_time_in_mts
        
        parsed_msg = {
            'data': {
                'ts': current_time_in_mts / 1000,  # convert to seconds
                'price': msg['price'],
                'volume': volume,
            },
            'extra_data': {
                'exchange': msg['exchange'],
                'market_hours': msg['market_hours'],
                'last_size': msg.get('last_size', None),
            }
        }
        return parsed_msg
    
    def _add_data_channel(self, data_model: YahooFinanceMarketDataModel) -> None:
        return self.data_source.stream_api._add_data_channel(data_model)
    
    def add_channel(self, channel: FullDataChannel, channel_type: Literal['public', 'private'], *args, **kwargs):
        raise NotImplementedError(f'{self.name} add_channel() is not implemented')
    
    # TODO: use data_source.batch_api
    def _fetch_impl(self, data_model: YahooFinanceMarketDataModel, *args, **kwargs) -> GenericFrameOrNone:
        raise NotImplementedError(f'{self.name} _fetch_impl() is not implemented')

    # DEPRECATED
    # def get_historical_data(
    #     self,
    #     product: str,
    #     symbol: str='',
    #     resolution: Resolution | str = "1day",
    #     rollback_period: str | Literal["ytd", "max"] = "max",
    #     start_date: str = "",
    #     end_date: str = "",
    #     data_origin: str='',
    #     data_layer: tDataLayer | None=None,
    #     data_domain: str='',
    #     from_storage: tStorage | None=None,
    #     to_storage: tStorage | None=None,
    #     storage_options: dict | None=None,
    #     force_download: bool=False,
    #     retrieve_per_date: bool=False,
    #     yfinance_kwargs: dict | None=None,
    #     **product_specs,
    # ) -> GenericFrameOrNone:
    #     """Gets historical data from Yahoo Finance using yfinance's Ticker.history().
    #     Args:
    #         product: product basis, e.g. AAPL_USD_STK, BTC_USDT_PERP
    #         symbol: symbol that will be used by yfinance's Ticker.history().
    #             If not specified, it will be derived from `product`, which might be inaccurate.
    #         rollback_period: Data resolution or 'ytd' or 'max'
    #             Period to rollback from today, only used when `start_date` is not specified.
    #             Default is '1M' = 1 month.
    #             if 'period' in kwargs is specified, it will be used instead of `rollback_period`.
    #         resolution: Data resolution
    #             e.g. '1m' = 1 minute as the unit of each data bar/candle.
    #             Default is '1d' = 1 day.
    #         force_download: Whether to skip retrieving data from storage.
    #         yfinance_kwargs: kwargs supported by `yfinance`
    #             refer to kwargs in history() in yfinance/scrapers/history.py
    #     """
    #     self._yfinance_kwargs = self._check_yfinance_kwargs(yfinance_kwargs)
    #     if rollback_period == 'max' and not start_date:
    #         start_date, end_date, rollback_period = self._handle_rollback_max_period(resolution, start_date, end_date)
    #         # HACK: for daily data with rollback_period='max', retrieving data from storage takes too long (too many dates), skip it
    #         if start_date == '1900-01-01':
    #             force_download = True
    #     df = super().get_historical_data(
    #         product,
    #         resolution,
    #         symbol=symbol,
    #         rollback_period=rollback_period,
    #         start_date=start_date,
    #         end_date=end_date,
    #         data_origin=data_origin,
    #         data_layer=data_layer,
    #         data_domain=data_domain,
    #         from_storage=from_storage,
    #         to_storage=to_storage,
    #         storage_options=storage_options,
    #         force_download=force_download,
    #         retrieve_per_date=retrieve_per_date,
    #         **product_specs,
    #     )
    #     self._yfinance_kwargs.clear()
    #     return df


    ###
    # Functions using yfinance for convenience
    ###
    def get_option_expirations(self, symbol: str) -> tuple[str]:
        '''Get all available option expirations for a given symbol.'''
        ticker: Ticker = self.batch_api.Ticker(symbol)
        expirations = ticker.options
        return expirations

    # TODO: standardize the df? e.g. standardize column names, date format etc.
    def get_option_chain(self, symbol: str, expiration: str, option_type: Literal['CALL', 'PUT']) -> GenericFrame:
        '''Get the option chain for a given symbol, expiration, and option type.
        Args:
            expiration: e.g. '2024-12-13', it must be one of the values returned by `get_option_expirations`.
            option_type: 'CALL' or 'PUT'
        '''
        from pfeed._etl.base import convert_to_user_df
        
        ticker: Ticker = self.batch_api.Ticker(symbol)
        option_chain = ticker.option_chain(expiration)
        if option_type.upper() == 'CALL':
            df = option_chain.calls
        elif option_type.upper() == 'PUT':
            df = option_chain.puts
        else:
            raise ValueError(f"Invalid option type: {option_type}")
        return convert_to_user_df(df, self._data_tool)
    