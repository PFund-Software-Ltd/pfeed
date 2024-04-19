from importlib.metadata import version

from pfeed.config_handler import configure


# NOTE: dynamically import modules to avoid click cli latency (reduced from ~4s to ~0.2s)
def __getattr__(name):
    """
    Dynamically import and return modules and classes based on their name.
    
    Supports dynamic loading of data sources and feed classes to minimize
    initial load time.

    Supported Names:
    - "bybit" -> Dynamically imports from pfeed.sources.bybit
    - Includes any class containing 'Feed' from pfeed.feeds
    """
    import importlib
    if 'Feed' in name:
        Feed = getattr(importlib.import_module('pfeed.feeds'), name)
        globals()[name] = Feed
        return Feed
    else:
        name = name.lower()
        data_source = importlib.import_module(f'pfeed.sources.{name}')
        globals()[name] = data_source
        return data_source
    

# NOTE: dummy classes/modules for type hinting
# e.g. import pfeed as pe, when you type "pe.", 
# you will still see the following suggestions even they are dynamically imported:
bybit: ...
YahooFinanceFeed: ...
BybitFeed: ...


__version__ = version('pfeed')
__all__ = (
    '__version__',
    'configure',
    'bybit',
    'YahooFinanceFeed',
    'BybitFeed',
)