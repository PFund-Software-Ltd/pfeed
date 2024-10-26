from pfund.const.common import (
    SUPPORTED_CRYPTO_PRODUCT_TYPES, 
    SUPPORTED_TRADFI_PRODUCT_TYPES
)

from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS, tSUPPORTED_DATA_ENGINES


SUPPORTED_ENVIRONMENTS = ['BACKTEST', 'SANDBOX', 'PAPER', 'LIVE']
SUPPORTED_DATA_FEEDS = ['YAHOO_FINANCE', 'BYBIT', 'BINANCE']
SUPPORTED_STORAGES = ['local', 'minio']
SUPPORTED_DOWNLOAD_DATA_SOURCES = ['BYBIT', 'BINANCE']
SUPPORTED_CRYPTO_EXCHANGES = ['BYBIT', 'BINANCE']
SUPPORTED_DATA_TOOLS: dict[tSUPPORTED_DATA_TOOLS, list[tSUPPORTED_DATA_ENGINES] | bool] = {
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
SUPPORTED_DATA_TYPES = [
    'quote_L1', 'quote_L2', 'quote_L3',
    'tick', 'second', 'minute', 'hour',
    'daily', 'weekly', 'monthly', 'yearly'
]
SUPPORTED_PRODUCT_TYPES = SUPPORTED_CRYPTO_PRODUCT_TYPES + SUPPORTED_TRADFI_PRODUCT_TYPES
