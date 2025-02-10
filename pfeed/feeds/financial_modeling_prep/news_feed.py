from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from fmp_api_client.news import News
    from pfeed.data_models.news_data_model import NewsDataModel
    from pfeed.flows.dataflow import DataFlow

import asyncio
import datetime

import pandas as pd
from fmp_api_client import FMPPlan

from pfeed.feeds.news_feed import NewsFeed


class FinancialModelingPrepNewsFeed(NewsFeed):
    
    @property
    def api(self) -> News:
        return self.data_source.api.news
    
    # TODO
    def _normalize_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO: parse the data
        # title: str=''
        # content: str=''
        # publisher: str=''
        # url: AnyHttpUrl=''
        pass
    
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

    def _execute_download(self, data_model: NewsDataModel) -> list[dict] | None:
        self.logger.debug(f'downloading {data_model}')
        data = asyncio.run(self._aexecute_download(data_model))
        if not data:
            self.logger.warning(f'no data downloaded for {data_model}')
            return None
        self.logger.debug(f'downloaded {data_model}')
        return data

    async def _aexecute_download(self, data_model: NewsDataModel) -> list[dict] | None:
        product = data_model.product
        start_date, end_date = str(data_model.start_date), str(data_model.end_date)
        tasks = []
        if product is None:
            if self.data_source.plan >= FMPPlan.STARTER:
                tasks.append(self.api.ageneral_news(from_=start_date, to=end_date))
            symbols = []
        else:
            symbols = [product.symbol]
        if self.data_source.plan >= FMPPlan.BASIC:
            tasks.append(
                self.api.aFMP_articles(symbols=symbols, start_date=start_date, end_date=end_date)
            )
        # TODO: find a way to read the url and get the actual contents
        # NOTE: remember there are restrictions on the "from_" and "to" parameters based on the plan
        # e.g. STARTER plan can only go back 6 months
        if self.data_source.plan >= FMPPlan.STARTER:
            if product.is_stock():
                tasks.append(self.api.asearch_stock_news(symbols=symbols, from_=start_date, to=end_date))
            elif product.is_crypto():
                tasks.append(self.api.asearch_crypto_news(symbols=symbols, from_=start_date, to=end_date))
            elif product.is_fx():
                tasks.append(self.api.asearch_forex_news(symbols=symbols, from_=start_date, to=end_date))
        if self.data_source.plan >= FMPPlan.PREMIUM:
            tasks.append(self.api.asearch_press_releases(symbols=symbols, from_=start_date, to=end_date))
        return await asyncio.gather(*tasks)

    # TODO:
    def retrieve(self):
        pass
    
    # TODO:
    def fetch(self):
        pass
