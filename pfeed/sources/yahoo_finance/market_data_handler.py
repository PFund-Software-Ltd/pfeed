import datetime

from pfeed.data_handlers.market_data_handler import MarketDataHandler


class YahooFinanceMarketDataHandler(MarketDataHandler):
    pass
    # TODO: handle raw data streaming
    # def _standardize_streaming_data(self, data: dict) -> dict:
    #     mts = data['ts']  # in ms
    #     date = datetime.datetime.fromtimestamp(
    #         mts / 1000,
    #         tz=datetime.timezone.utc
    #     ).replace(tzinfo=None)
    #     data['date'] = date

    #     # add year, month, day columns for delta table partitioning
    #     data['year'] = date.year
    #     data['month'] = date.month
    #     data['day'] = date.day 
    #     return data
    