from pfeed.feeds.pfund.pfund_feed import PFundFeed


# TODO: get around the data source issue, since technically its data source is the engine.
# this feed should be able to get backtesting data from pfund's BacktestEngine for monitoring and analysis puporse
# some functions require api calls (e.g. get dynamic backtest results) and some do not (e.g. load backtest hisory)
class BacktestingFeed(PFundFeed):
    pass