from pfeed.sources.binance.const import DATA_SOURCE as name
from pfeed.sources.binance.download import download_historical_data, download_historical_data as download
from pfeed.sources.binance.stream import stream_realtime_data, stream_realtime_data as stream


def __getattr__(name):
    if name in ('Feed', 'BinanceFeed'):
        from pfeed.feeds.binance_feed import BinanceFeed as Feed
        return Feed
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")