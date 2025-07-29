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
class BybitDataHandler(MarketDataHandler):
    pass