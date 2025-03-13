from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from pfeed.data_models.news_data_model import NewsDataModel

import datetime

from pfeed.feeds.news_feed import NewsFeed


class YahooFinanceNewsFeed(NewsFeed):
    # REVIEW: better handle some potentially useful attributes: "storyline" and "editorsPick"
    @staticmethod
    def _normalize_raw_data(data: list[dict]) -> pd.DataFrame:
        import pandas as pd
        # extract contents only
        data = [item['content'] for item in data]
        for item in data:
            item['url'] = item['canonicalUrl']['url']
        df = pd.DataFrame(data)
        if not df.empty:
            df.rename(columns={'pubDate': 'date', 'summary': 'content'}, inplace=True)
            df['publisher'] = 'Yahoo Finance'
        else:
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
        return df
    
    def _download_impl(self, data_model: NewsDataModel) -> dict[str, list[list[dict] | []]]:
        self.logger.debug(f'downloading {data_model}')
        symbol = data_model.product.symbol
        assert symbol, f'symbol is required for {data_model}'
        ticker = self.api.Ticker(symbol)
        # FIXME: hard-code count to 100
        data = ticker.get_news(count=100, tab='news')

        # filter data by start_date and end_date
        start_date, end_date = data_model.start_date, data_model.end_date
        data = [
            item for item in data
            if start_date <= datetime.datetime.strptime(item['content']['pubDate'], '%Y-%m-%dT%H:%M:%SZ').date() <= end_date
        ]
        self.logger.debug(f'downloaded {data_model}')
        return data
