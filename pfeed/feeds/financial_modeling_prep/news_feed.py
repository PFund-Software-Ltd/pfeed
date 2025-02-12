from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from fmp_api_client.news import News
    from pfeed.data_models.news_data_model import NewsDataModel
    from pfeed.flows.dataflow import DataFlow
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_LAYER, tSTORAGE
    
import asyncio
import datetime

import pandas as pd
from fmp_api_client import FMPPlan

from pfeed.feeds.news_feed import NewsFeed


class FinancialModelingPrepNewsFeed(NewsFeed):
    
    @property
    def api(self) -> News:
        return self.data_source.api.news
    
    def _normalize_raw_data(self, datas: dict[str, list[list[dict] | []]]) -> pd.DataFrame:
        for func, data in datas.items():
            if func == 'FMP_articles':
                df = self._normalize_raw_data_from_FMP_articles(data)
            elif func in ('search_stock_news', 'search_crypto_news', 'search_forex_news'):
                df = self._normalize_raw_data_from_search(data)
            # TODO: press releases
            # elif func == 'search_press_releases':
            #     df = self._normalize_raw_data_from_search_press_releases(data)
            else:
                raise ValueError(f'unknown function {func}')
        return df
    
    def _normalize_raw_data_from_FMP_articles(self, data: list[list[dict] | []]) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.rename(columns={'site': 'publisher', 'link': 'url'}, inplace=True)
        df[["exchange", "symbol"]] = df["tickers"].str.split(":", expand=True)
        return df
    
    def _normalize_raw_data_from_search(self, data: list[list[dict] | []]) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.rename(columns={'publishedDate': 'date', 'text': 'content'}, inplace=True)
        return df
    
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
    ) -> tDataFrame | None | NewsFeed:
        return super().download(
            product=product,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            data_layer=data_layer,
            data_domain=data_domain,
            to_storage=to_storage,
            auto_transform=auto_transform,
            **product_specs
        )
    
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

    def _execute_download(self, data_model: NewsDataModel) -> dict[str, list[list[dict] | []]]:
        self.logger.debug(f'downloading {data_model}')
        datas: dict[str, list[list[dict] | []]] = asyncio.run(self._aexecute_download(data_model))
        for func, data in datas.items():
            if not data:
                self.logger.warning(f'no data downloaded using function {func} for {data_model}')
            else:
                self.logger.debug(f'downloaded using function {func} for {data_model}')
        return datas

    async def _aexecute_download(self, data_model: NewsDataModel) -> dict[str, list[list[dict] | []]]:
        product = data_model.product
        start_date, end_date = str(data_model.start_date), str(data_model.end_date)
        tasks = {}
        if product is None:
            if self.data_source.plan >= FMPPlan.STARTER:
                tasks.append(self.api.ageneral_news(from_=start_date, to=end_date))
            symbols = []
        else:
            symbols = [product.symbol]
        if self.data_source.plan >= FMPPlan.BASIC:
            # NOTE: STARTER/PREMIUM plans can only get 1 article per response...
            tasks['FMP_articles'] = self.api.aFMP_articles(symbols=symbols, start_date=start_date, end_date=end_date)
        # TODO: find a way to read the url and get the actual contents
        # NOTE: remember there are restrictions on the "from_" and "to" parameters based on the plan
        # e.g. STARTER plan can only go back 6 months
        if self.data_source.plan >= FMPPlan.STARTER:
            if product.is_stock():
                tasks['search_stock_news'] = self.api.asearch_stock_news(symbols=symbols, from_=start_date, to=end_date)
            elif product.is_crypto():
                tasks['search_crypto_news'] = self.api.asearch_crypto_news(symbols=symbols, from_=start_date, to=end_date)
            elif product.is_fx():
                tasks['search_forex_news'] = self.api.asearch_forex_news(symbols=symbols, from_=start_date, to=end_date)
        if self.data_source.plan >= FMPPlan.PREMIUM:
            tasks['search_press_releases'] = self.api.asearch_press_releases(symbols=symbols, from_=start_date, to=end_date)
        results = await asyncio.gather(*tasks.values())
        return dict(zip(tasks.keys(), results))

    # TODO:
    def retrieve(self):
        pass
    
    # TODO:
    def fetch(self):
        pass
