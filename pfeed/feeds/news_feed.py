from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfund.products.product_base import BaseProduct
    from pfeed.flows.dataflow import DataFlow
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_LAYER, tSTORAGE, tENVIRONMENT

import datetime

import pandas as pd
import narwhals as nw
from rich.console import Console

from pfeed.feeds.base_feed import BaseFeed, clear_subflows
from pfeed.data_models.news_data_model import NewsDataModel
from pfeed._etl import news as etl
from pfeed.const.enums import DataStorage
from pfeed._etl.base import convert_to_user_df
from pfeed.storages.base_storage import BaseStorage
from pfeed.utils.utils import lambda_with_name, validate_product
from pfeed.utils.dataframe import is_empty_dataframe


'''
# FIXME: the core issue with NewsFeed is the return of each API call could be different
i.e. it makes the data stored per date NOT deterministic
e.g. if getting today's data, the next API call could have more data, 
e.g. if changing the params for the same API call, the data could be different.
how to handle this? handled by metadata?
'''
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
        if isinstance(product, str) and product:
            product = self.create_product(product, **product_specs)
        if isinstance(start_date, str) and start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str) and end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        return NewsDataModel(
            env=env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=product,
            start_date=start_date,
            end_date=end_date or start_date,
        )
    
    def create_storage(
        self,
        storage: tSTORAGE,
        start_date: str | datetime.date,
        end_date: str | datetime.date | None = None,
        product: str | BaseProduct | None = None,
        data_origin: str = '',
        env: tENVIRONMENT = 'BACKTEST',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        storage_configs: dict | None=None,
        **product_specs
    ) -> BaseStorage:
        data_model = self.create_data_model(
            start_date,
            end_date=end_date,
            product=product,
            data_origin=data_origin,
            env=env,
            **product_specs
        )
        storage_configs = storage_configs or {}
        Storage = DataStorage[storage.upper()].storage_class
        return Storage.from_data_model(
            data_model,
            data_layer,
            data_domain=data_domain or self.DATA_DOMAIN,
            use_deltalake=self._use_deltalake,
            **storage_configs,
        )

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
        storage_configs: dict | None=None,
        auto_transform: bool=True,
        **product_specs
    ) -> tDataFrame | None | NewsFeed:
        '''
        Args:
            product: e.g. 'AAPL_USD_STK'. If not provided, general news will be fetched.
        '''
        validate_product(product, allow_empty=True)
        if not product:
            assert not symbol, 'symbol should not be provided if product is not provided'
        else:
            product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        # if no default and no custom transformations, set data_layer to 'raw'
        if not auto_transform and not self._pipeline_mode:
            data_layer = 'raw' 
        if start_date and end_date:
            Console().print(f'Downloading historical news data from {self.name}, from {str(start_date)} to {str(end_date)} (UTC), {data_layer=}', style='bold yellow')
        self._create_download_dataflows(
            start_date,
            end_date,
            product=product,
            data_origin=data_origin,
        )
        if auto_transform:
            self._add_default_transformations_to_download(product=product)
        if not self._pipeline_mode:
            self.load(
                to_storage=to_storage,
                data_layer=data_layer,
                data_domain=data_domain,
                storage_configs=storage_configs,
            )
            completed_dataflows, failed_dataflows = self.run()
            if completed_dataflows:
                dfs: list[Frame] = [nw.from_native(dataflow.output) for dataflow in completed_dataflows]
                df: Frame = nw.concat(df for df in dfs if not is_empty_dataframe(df))
                df: tDataFrame = nw.to_native(df)
            else:
                df = None
            return df
        else:
            return self
    
    def _create_download_dataflows(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        product: BaseProduct | None=None,
        data_origin: str='',
    ) -> list[DataFlow]:
        # NOTE: one data model for the entire date range
        data_model = self.create_data_model(
            start_date=start_date,
            end_date=end_date,
            product=product,
            data_origin=data_origin,
        )
        # create a dataflow that schedules _execute_download()
        dataflow = self._extract_download(data_model)
        return [dataflow]

    def _add_default_transformations_to_download(self, product: BaseProduct | None=None):
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
                lambda df: convert_to_user_df(df, self.data_tool.name)
            )
        )
        
    @clear_subflows
    def retrieve(
        self,
        product: str='',
        rollback_period: str='1w',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
        auto_transform: bool=True,
        **product_specs
    ) -> tDataFrame | None | NewsFeed:
        validate_product(product, allow_empty=True)
        if product:
            product: BaseProduct = self.create_product(product, **product_specs)
        else:
            product = None
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        self._create_retrieve_dataflows(
            start_date,
            end_date,
            data_origin,
            data_layer,
            data_domain or self.DATA_DOMAIN,
            product=product,
            from_storage=from_storage,
            storage_configs=storage_configs,
        )
        if auto_transform:
            self._add_default_transformations_to_retrieve()
        if not self._pipeline_mode:
            completed_dataflows, failed_dataflows = self.run()
            if completed_dataflows:
                dfs: list[Frame] = [nw.from_native(dataflow.output) for dataflow in completed_dataflows]
                df: Frame = nw.concat(df for df in dfs if not is_empty_dataframe(df))
                df: tDataFrame = nw.to_native(df)
            else:
                df = None
            return df
        else:
            return self
    
    def _create_retrieve_dataflows(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        data_origin: str,
        data_layer: tDATA_LAYER,
        data_domain: str,
        product: BaseProduct | None=None,
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
    ) -> list[DataFlow]:
        dataflows: list[DataFlow] = []
        # NOTE: one data model per date
        for date in pd.date_range(start_date, end_date).date:
            data_model = self.create_data_model(
                start_date=date,
                product=product,
                data_origin=data_origin,
            )
            dataflow: DataFlow = self._extract_retrieve(
                data_model,
                data_layer,
                data_domain,
                from_storage=from_storage,
                storage_configs=storage_configs,
            )
            dataflows.append(dataflow)
        return dataflows
    
    def _add_default_transformations_to_retrieve(self):
        self.transform(
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_user_df(df, self.data_tool.name)
            )
        )

    # TODO
    def get_historical_data(
        self, 
        product: str='',
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
        validate_product(product, allow_empty=True)
    
    # TODO
    def fetch(self):
        pass