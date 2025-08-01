from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfund._typing import tEnvironment
    from pfeed._typing import tDataLayer, tStorage, GenericFrame, StorageMetadata
    from pfeed.data_models.news_data_model import NewsDataModel

import datetime
from functools import partial

from pfund.enums import Environment
from pfeed.enums import DataCategory, DataLayer
from pfeed.feeds.time_based_feed import TimeBasedFeed
from pfeed.utils.utils import lambda_with_name


'''
# FIXME: the core issue with NewsFeed is the return of each API call could be different
i.e. it makes the data stored per date NOT deterministic
e.g. if getting today's data, the next API call could have more data, 
e.g. if changing the params for the same API call, the data could be different.
how to handle this? handled by metadata?
'''
class NewsFeed(TimeBasedFeed):
    data_domain = DataCategory.NEWS_DATA

    def create_data_model(
        self,
        start_date: str | datetime.date,
        end_date: str | datetime.date | None = None,
        product: str | BaseProduct | None = None,
        data_origin: str = '',
        env: tEnvironment | None = None,
        **product_specs
    ) -> NewsDataModel:
        from pfeed.data_models.news_data_model import NewsDataModel
        if isinstance(product, str) and product:
            product = self.create_product(product, **product_specs)
        if isinstance(start_date, str) and start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str) and end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        return NewsDataModel(
            env=env or self._env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=product,
            start_date=start_date,
            end_date=end_date or start_date,
            use_deltalake=self._use_deltalake
        )
    
    def download(
        self, 
        product: str='',
        symbol: str='',
        rollback_period: str ='1w',
        start_date: str='',
        end_date: str='',
        data_layer: tDataLayer='CLEANED',
        data_origin: str='',
        to_storage: tStorage | None='LOCAL',
        storage_options: dict | None=None,
        auto_transform: bool=True,
        dataflow_per_date: bool=False,
        include_metadata: bool=False,
        **product_specs
    ) -> GenericFrame | None | tuple[GenericFrame | None, StorageMetadata] | NewsFeed:
        '''
        Args:
            product: e.g. 'AAPL_USD_STK'. If not provided, general news will be fetched.
        '''
        env = Environment.BACKTEST
        if not product:
            assert not symbol, 'symbol should not be provided if product is not provided'
            product = None
        else:
            product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        data_layer, data_domain = DataLayer[data_layer.upper()], self.data_domain.value
        return self._run_download(
            partial_dataflow_data_model=partial(self.create_data_model, product=product, data_origin=data_origin, env=env),
            partial_faucet_data_model=partial(self.create_data_model, product=product, data_origin=data_origin, env=env),
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            to_storage=to_storage,
            storage_options=storage_options,
            add_default_transformations=(lambda: self._add_default_transformations_to_download(product=product)) if auto_transform else None,
            dataflow_per_date=dataflow_per_date,
            include_metadata=include_metadata,
        )
    
    def _add_default_transformations_to_download(self, product: BaseProduct | None=None):
        from pfeed._etl import news as etl
        from pfeed._etl.base import convert_to_user_df
        self.transform(
            self._normalize_raw_data,
            lambda_with_name(
                'standardize_columns',
                lambda df: etl.standardize_columns(df, product=product),
            ),
            etl.filter_columns,
            etl.organize_columns,
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_user_df(df, self._data_tool)
            )
        )
        
    def retrieve(
        self,
        product: str='',
        rollback_period: str='1w',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDataLayer='CLEANED',
        data_domain: str='',
        from_storage: tStorage | None=None,
        storage_options: dict | None=None,
        auto_transform: bool=True,
        dataflow_per_date: bool=False,
        include_metadata: bool=False,
        env: tEnvironment | None = None,
        **product_specs
    ) -> GenericFrame | None | tuple[GenericFrame | None, StorageMetadata] | NewsFeed:
        product: BaseProduct | None = self.create_product(product, **product_specs) if product else None
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        data_domain = data_domain or self.data_domain.value
        if env:
            env = Environment[env.upper()]
        return self._run_retrieve(
            partial_dataflow_data_model=partial(self.create_data_model, product=product, data_origin=data_origin, env=env),
            partial_faucet_data_model=partial(self.create_data_model, product=product, data_origin=data_origin, env=env),
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            from_storage=from_storage,
            storage_options=storage_options,
            add_default_transformations=(lambda: self._add_default_transformations_to_retrieve()) if auto_transform else None,
            dataflow_per_date=dataflow_per_date,
            include_metadata=include_metadata,
        )

    def _add_default_transformations_to_retrieve(self):
        from pfeed._etl.base import convert_to_user_df
        self.transform(
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_user_df(df, self._data_tool)
            )
        )

    def get_historical_data(
        self, 
        product: str='',
        symbol: str='', 
        rollback_period: str="1w",
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDataLayer | None=None,
        data_domain: str='',
        from_storage: tStorage | None=None,
        to_storage: tStorage | None=None,
        storage_options: dict | None=None,
        force_download: bool=False,
        retrieve_per_date: bool=False,
        **product_specs
    ) -> GenericFrame | None:
        '''
        Get news data from data source.
        Data will be stored in cache by default.
        Args:
            NOTE: this behavior is different from MarketFeed
            from_storage: if from_storage is not specified, data will be fetched again from data source.
        '''
        data_domain = data_domain or self.data_domain.value
        return self._get_historical_data_impl(
            product=product,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            data_layer=data_layer,
            data_domain=data_domain,
            from_storage=from_storage,
            to_storage=to_storage,
            storage_options=storage_options,
            force_download=force_download,
            retrieve_per_date=retrieve_per_date,
            product_specs=product_specs,
        )
            
    # TODO:
    def fetch(self) -> GenericFrame | None | NewsFeed:
        raise NotImplementedError(f"{self.name} fetch() is not implemented")