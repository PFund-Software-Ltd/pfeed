from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import pandas as pd
    from pfeed.typing import tStorage, tDataLayer, GenericFrameOrNone
    from pfeed.data_models.news_data_model import NewsDataModel

import datetime

from pfeed.feeds.news_feed import NewsFeed
from pfeed.sources.yahoo_finance.mixin import YahooFinanceMixin


__all__ = ["YahooFinanceNewsFeed"]


class YahooFinanceNewsFeed(YahooFinanceMixin, NewsFeed):
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
        num_news: int=10,
        news_type: Literal['news', 'press releases', 'all']='news',
        **product_specs
    ) -> GenericFrameOrNone | NewsFeed:
        '''
        Args: 
            num_news: number of news to fetch, it is equivalent to the argument 'count' in yfinance.get_news()
            news_type: type of news to fetch, can be 'news', 'press releases', or 'all', it is equivalent to the argument 'tab' in yfinance.get_news()
        '''
        self._yfinance_kwargs = {
            'count': num_news,
            'tab': news_type,
        }
        return super().download(
            product=product,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_origin=data_origin,
            to_storage=to_storage,
            storage_options=storage_options,
            dataflow_per_date=False,
            include_metadata=False,
            **product_specs
        )
        
    def _download_impl(self, data_model: NewsDataModel) -> dict[str, list[list[dict]]]:
        self.logger.debug(f'downloading {data_model}')
        symbol = data_model.product.symbol
        assert symbol, f'symbol is required for {data_model}'
        ticker = self.batch_api.Ticker(symbol)
        data = ticker.get_news(**self._yfinance_kwargs)
        # filter data by start_date and end_date
        start_date, end_date = data_model.start_date, data_model.end_date
        data = [
            item for item in data
            if start_date <= datetime.datetime.strptime(item['content']['pubDate'], '%Y-%m-%dT%H:%M:%SZ').date() <= end_date
        ]
        self.logger.debug(f'downloaded {data_model}')
        return data
