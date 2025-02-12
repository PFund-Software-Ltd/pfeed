from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.data_models.news_data_model import NewsDataModel
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_LAYER, tSTORAGE

import datetime

import yfinance as yf
import pandas as pd

from pfeed.feeds.news_feed import NewsFeed


class YahooFinanceNewsFeed(NewsFeed):
    # REVIEW: better handle some potentially useful attributes: "storyline" and "editorsPick"
    def _normalize_raw_data(self, data) -> pd.DataFrame:
        # extract contents only
        data = [item['content'] for item in data]
        for item in data:
            item['url'] = item['canonicalUrl']['url']
        df = pd.DataFrame(data)
        df.rename(columns={'pubDate': 'date', 'summary': 'content'}, inplace=True)
        df['publisher'] = 'Yahoo Finance'
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
    ) -> tDataFrame | None | YahooFinanceNewsFeed:
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
    
    def _execute_download(self, data_model: NewsDataModel) -> dict[str, list[list[dict] | []]]:
        self.logger.debug(f'downloading {data_model}')
        symbol = data_model.product.symbol
        assert symbol, f'symbol is required for {data_model}'
        ticker = yf.Ticker(symbol)
        # FIXME: hard-code count to 10
        data = ticker.get_news(count=10, tab='news')

        # filter data by start_date and end_date
        start_date, end_date = data_model.start_date, data_model.end_date
        data = [
            item for item in data
            if start_date <= datetime.datetime.strptime(item['content']['pubDate'], '%Y-%m-%dT%H:%M:%SZ').date() <= end_date
        ]
        self.logger.debug(f'downloaded {data_model}')
        return data
    
    # TODO
    def retrieve(self):
        pass
    
    # TODO
    def fetch(self):
        pass