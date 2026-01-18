from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfeed._io.base_io import StorageMetadata
    from pfeed.typing import GenericFrameOrNone
    from pfeed.data_models.news_data_model import NewsDataModel
    from pfeed.enums import DataStorage

import datetime
from functools import partial

from pfund.enums import Environment
from pfeed.config import setup_logging
from pfeed.enums import DataLayer
from pfeed.feeds.time_based_feed import TimeBasedFeed
from pfeed.utils import lambda_with_name


'''
# FIXME: the core issue with NewsFeed is the return of each API call could be different
i.e. it makes the data stored per date NOT deterministic
e.g. if getting today's data, the next API call could have more data, 
e.g. if changing the params for the same API call, the data could be different.
how to handle this? handled by metadata?
'''
class NewsFeed(TimeBasedFeed):
    data_model_class: ClassVar[type[NewsDataModel]] = NewsDataModel

    def create_data_model(
        self,
        env: Environment,
        start_date: str | datetime.date,
        end_date: str | datetime.date | None = None,
        product: str | BaseProduct | None = None,
        data_origin: str = '',
        **product_specs
    ) -> NewsDataModel:
        from pfeed.data_models.news_data_model import NewsDataModel
        if isinstance(product, str) and product:
            product = self.create_product(product, **product_specs)
        if len(self._dataflows) > 0:
            existing_env = self._dataflows[0].data_model.env
            if existing_env != env:
                raise ValueError(f'{self.name} dataflows have different environments: {existing_env} and {env}')
        return NewsDataModel(
            env=env,
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
        data_layer: DataLayer=DataLayer.CLEANED,
        data_origin: str='',
        to_storage: DataStorage=DataStorage.LOCAL,
        dataflow_per_date: bool=False,
        include_metadata: bool=False,
        **product_specs
    ) -> GenericFrameOrNone | tuple[GenericFrameOrNone, StorageMetadata] | NewsFeed:
        '''
        Args:
            product: e.g. 'AAPL_USD_STK'. If not provided, general news will be fetched.
        '''
        env = Environment.BACKTEST
        setup_logging(env=env)
        if not product:
            assert not symbol, 'symbol should not be provided if product is not provided'
            product = None
        else:
            product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        data_layer = DataLayer[data_layer.upper()]
        return self._run_download(
            partial_dataflow_data_model=partial(self.create_data_model, env=env, product=product, data_origin=data_origin),
            partial_faucet_data_model=partial(self.create_data_model, env=env, product=product, data_origin=data_origin),
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=dataflow_per_date,
            include_metadata=include_metadata,
            add_default_transformations=lambda: self._add_default_transformations_to_download(data_layer, product=product),
            load_to_storage=(lambda: self.load(to_storage, data_layer)) if to_storage else None,
        )
    
    def _add_default_transformations_to_download(self, data_layer: DataLayer, product: BaseProduct | None=None):
        from pfeed._etl import news as etl
        from pfeed._etl.base import convert_to_desired_df
        if data_layer != DataLayer.RAW:
            self.transform(
                self._normalize_raw_data,
                lambda_with_name(
                    'standardize_columns',
                    lambda df: etl.standardize_columns(df, product=product),
                ),
                etl.organize_columns,
            )
        self.transform(
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_desired_df(df, self._data_tool)
            )
        )
        
    def retrieve(
        self,
        product: str='',
        rollback_period: str='1w',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: DataLayer=DataLayer.CLEANED,
        from_storage: DataStorage=DataStorage.LOCAL,
        dataflow_per_date: bool=False,
        include_metadata: bool=False,
        env: Environment=Environment.BACKTEST,
        **product_specs
    ) -> GenericFrameOrNone | tuple[GenericFrameOrNone, StorageMetadata] | NewsFeed:
        env = Environment[env.upper()]
        setup_logging(env=env)
        product: BaseProduct | None = self.create_product(product, **product_specs) if product else None
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        env = Environment[env.upper()]
        return self._run_retrieve(
            partial_dataflow_data_model=partial(self.create_data_model, env=env, product=product, data_origin=data_origin),
            partial_faucet_data_model=partial(self.create_data_model, env=env, product=product, data_origin=data_origin),
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            from_storage=from_storage,
            add_default_transformations=lambda: self._add_default_transformations_to_retrieve(),
            dataflow_per_date=dataflow_per_date,
            include_metadata=include_metadata,
        )

    # TODO:
    def fetch(self) -> GenericFrameOrNone | NewsFeed:
        raise NotImplementedError(f"{self.name} fetch() is not implemented")

    # DEPRECATED
    # def get_historical_data(
    #     self, 
    #     product: str='',
    #     symbol: str='', 
    #     rollback_period: str="1w",
    #     start_date: str='',
    #     end_date: str='',
    #     data_origin: str='',
    #     data_layer: DataLayer | None=None,
    #     data_domain: str='',
    #     from_storage: DataStorage | None=None,
    #     to_storage: DataStorage | None=None,
    #     storage_options: dict | None=None,
    #     force_download: bool=False,
    #     retrieve_per_date: bool=False,
    #     **product_specs
    # ) -> GenericFrameOrNone:
    #     '''
    #     Get news data from data source.
    #     Data will be stored in cache by default.
    #     Args:
    #         NOTE: this behavior is different from MarketFeed
    #         from_storage: if from_storage is not specified, data will be fetched again from data source.
    #     '''
    #     data_domain = data_domain or self.data_domain.value
    #     return self._get_historical_data_impl(
    #         product=product,
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
    #         product_specs=product_specs,
    #     )
