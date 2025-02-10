from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfeed.typing.literals import tDATA_LAYER, tSTORAGE, tENVIRONMENT

import datetime

import pandas as pd

from pfeed.feeds.base_feed import BaseFeed, clear_subflows
from pfeed.data_models.news_data_model import NewsDataModel
from pfeed.storages.base_storage import BaseStorage
from pfeed.utils.utils import validate_product


class NewsFeed(BaseFeed):
    DATA_DOMAIN = 'news_data'

    def create_data_model(
        self,
        start_date: str | datetime.date,
        end_date: str | datetime.date | None = None,
        product: str | BaseProduct | None = None,
        data_origin: str = '',
        env: tENVIRONMENT = 'BACKTEST',
        **product_specs
    ) -> NewsDataModel:
        if isinstance(product, str):
            product = self.create_product(product, **product_specs)
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        return NewsDataModel(
            env=env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=product,
            start_date=start_date,
            end_date=end_date,
            **product_specs
        )
    
    def create_storage(self, *args, **kwargs) -> BaseStorage:
        pass

    @validate_product
    @clear_subflows
    def download(
        self, 
        product: str='',
        symbol: str='',
        rollback_period: str ='1w',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        to_storage: tSTORAGE='local',
        auto_transform: bool=True,
        **product_specs
    ):
        '''
        Args:
            product: e.g. 'AAPL_USD_STK'. If not provided, general news will be fetched.
        '''
        if not product:
            assert not symbol, 'symbol should not be provided if product is not provided'
        

    @validate_product
    def get_historical_data(
        self, 
        product: str, 
        symbol: str='', 
        rollback_period: str="1w",
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        from_storage: tSTORAGE | None=None,
        **product_specs
    ):
        '''
        Get news data from data source.
        Data will be stored in cache by default.
        If from_storage is not specified, data will be fetched again from data source.
        '''
        pass
