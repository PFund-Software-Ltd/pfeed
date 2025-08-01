import datetime

from pfeed.data_handlers.market_data_handler import MarketDataHandler


# TODO:
'''
current problem is during streaming, e.g. subscribed to 1m bar,
bar update from bybit is per trade (or per second, not sure)
the bar is only complete when the bar update has "confirm" = true
so we probably need sth like BybitDataHandler to do some work:
- when reading streaming data, only retrieve complete bars
- and more ...
'''
class BybitMarketDataHandler(MarketDataHandler):
    def _standardize_streaming_data(self, data: dict) -> dict:
        mts = data['ts']  # in ms
        date = datetime.datetime.fromtimestamp(
            mts / 1000,
            tz=datetime.timezone.utc
        ).replace(tzinfo=None)
        data['date'] = date

        # add year, month, day columns for delta table partitioning
        data['year'] = date.year
        data['month'] = date.month
        data['day'] = date.day 
        return data
    