"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import datetime
    import pandas as pd
    from bytewax.dataflow import Stream as BytewaxStream
    from bytewax.inputs import Source as BytewaxSource
    from pfeed.typing import GenericFrame
    from pfund.datas.resolution import Resolution
    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.typing import tSTORAGE, tDATA_LAYER

from pfeed.feeds.crypto_market_feed import CryptoMarketFeed
from pfeed.sources.bybit.source import BybitSource


__all__ = ['BybitMarketFeed']


class BybitMarketFeed(CryptoMarketFeed):
    @staticmethod
    def _create_data_source() -> BybitSource:
        return BybitSource()
    
    @staticmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw data from Bybit API into standardized format.

        This method performs the following normalizations:
        - Renames columns to standard names (timestamp -> ts, size -> volume)
        - Maps trade side values from Buy/Sell to 1/-1
        - Corrects reverse-ordered data if detected

        Args:
            df (pd.DataFrame): Raw DataFrame from Bybit API containing trade data

        Returns:
            pd.DataFrame: Normalized DataFrame with standardized column names and values
        """
        MAPPING_COLS = {'Buy': 1, 'Sell': -1}
        RENAMING_COLS = {'timestamp': 'date', 'size': 'volume'}
        df = df.rename(columns=RENAMING_COLS)
        df['side'] = df['side'].map(MAPPING_COLS)
        return df

    def download(
        self,
        product: str, 
        resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day', 'max']='tick',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        data_layer: Literal['raw', 'cleaned']='cleaned',
        data_domain: str='',
        to_storage: tSTORAGE | None='local',
        storage_options: dict | None=None,
        auto_transform: bool=True,
        **product_specs
    ) -> GenericFrame | None | dict[datetime.date, GenericFrame | None] | BybitMarketFeed:
        '''
        Args:
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                download(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
        '''
        return super().download(
            product=product,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            to_storage=to_storage,
            storage_options=storage_options,
            auto_transform=auto_transform,
            dataflow_per_date=True,
            **product_specs
        )

    def _download_impl(self, data_model: MarketDataModel) -> bytes | None:
        self.logger.debug(f'downloading {data_model}')
        data = self.api.get_data(data_model.product, data_model.date)
        self.logger.debug(f'downloaded {data_model}')
        return data

    def get_historical_data(
        self,
        product: str,
        resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day']="1tick",
        rollback_period: str="1day",
        start_date: str="",
        end_date: str="",
        data_layer: tDATA_LAYER | None=None,
        data_domain: str='',
        data_origin: str='',
        from_storage: tSTORAGE | None=None,
        to_storage: tSTORAGE | None='cache',
        storage_options: dict | None=None,
        force_download: bool=False,
        **product_specs
    ) -> GenericFrame | None | BybitMarketFeed:
        return super().get_historical_data(
            product,
            resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            data_origin=data_origin,
            from_storage=from_storage,
            to_storage=to_storage,
            storage_options=storage_options,
            force_download=force_download,
            **product_specs
        )
        
    # TODO
    def stream(self) -> BybitMarketFeed:
        raise NotImplementedError(f'{self.name} stream() is not implemented')
        return self
    
    # TODO
    def _stream_impl(
        self, 
        data_model: MarketDataModel, 
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
    ):
        if self._use_bytewax:
            if bytewax_source is None:
                # TODO: derive bytewax source based on the feed
                pass
            return bytewax_source
        else:
            # TODO: start streaming
            pass