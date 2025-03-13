from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from fmp_api_client.news import News
    from pfeed.data_models.news_data_model import NewsDataModel
    
import asyncio

import pandas as pd
from fmp_api_client import FMPPlan

from pfeed.feeds.news_feed import NewsFeed


class FinancialModelingPrepNewsFeed(NewsFeed):
    
    @property
    def api(self) -> News:
        return self.data_source.api.news
    
    def _normalize_raw_data(self, datas: dict[str, list[list[dict] | []]]) -> pd.DataFrame:
        dfs = []
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

            if df.empty:
                # create an empty dataframe with required columns
                df = pd.DataFrame(columns=[
                    "date", "title", "content", "publisher", "url", "product", "symbol"
                ]).astype({
                    "date": "datetime64[ns]",
                    "title": "string",
                    "content": "string",
                    "publisher": "string",
                    "url": "string",
                    "product": "string",
                    "symbol": "string"
                })
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)
    
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
    
    def _download_impl(self, data_model: NewsDataModel) -> dict[str, list[list[dict] | []]]:
        self.logger.debug(f'downloading {data_model}')
        datas: dict[str, list[list[dict] | []]] = asyncio.run(self._adownload_impl(data_model))
        for func, data in datas.items():
            if not data:
                self.logger.warning(f'no data downloaded using function {func} for {data_model}')
            else:
                self.logger.debug(f'downloaded using function {func} for {data_model}')
        return datas

    async def _adownload_impl(self, data_model: NewsDataModel) -> dict[str, list[list[dict] | []]]:
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
