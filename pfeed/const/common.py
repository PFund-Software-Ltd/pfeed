from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS, tSUPPORTED_DATA_ENGINES


SUPPORTED_ENVIRONMENTS = ['BACKTEST', 'SANDBOX', 'PAPER', 'LIVE']
SUPPORTED_DATA_FEEDS = ['YAHOO_FINANCE', 'BYBIT', 'BINANCE']
SUPPORTED_STORAGES = ['local', 'minio']
SUPPORTED_DOWNLOAD_DATA_SOURCES = ['BYBIT', 'BINANCE']
SUPPORTED_CRYPTO_EXCHANGES = ['BYBIT', 'BINANCE']
SUPPORTED_DATA_TOOLS: dict[tSUPPORTED_DATA_TOOLS, tSUPPORTED_DATA_ENGINES | bool] = {
    'pandas': ['dask'],
    'polars': ['ray'],
    # True means the data tool is also an execution engine
    'dask': True,
    'spark': True,
}
SUPPORTED_DATA_ENGINES: dict[tSUPPORTED_DATA_ENGINES, list[str]] = {
    # execution engine: supported cloud services
    'dask': ['coiled'],
    'spark': ['databricks'],
    'ray': ['aws'],
}
SUPPORTED_CLOUDS = ['aws']
SUPPORTED_PRODUCT_TYPES = ['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
SUPPORTED_DATA_TYPES = [
    'raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily',
    'tick', 'second', 'minute', 'hour', 'daily',
]

ALIASES = {
    'YF': 'YAHOO_FINANCE',
    'FRD': 'FIRSTRATE_DATA'
}