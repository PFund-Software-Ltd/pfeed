from pfeed.sources.bybit.const import DATA_SOURCE as name
from pfeed.sources.bybit.download import download_historical_data, download_historical_data as download
from pfeed.sources.bybit.stream import stream_realtime_data, stream_realtime_data as stream


def __getattr__(name):
    if name in ('Feed', 'BybitFeed'):
        from pfeed.feeds.bybit_feed import BybitFeed as Feed
        return Feed
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")