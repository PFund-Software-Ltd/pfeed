from typing import Literal


# since Literal doesn't support variables as inputs, define variables in common.py here with prefix 't'
tSUPPORTED_ENVIRONMENTS = Literal['BACKTEST', 'SANDBOX', 'PAPER', 'LIVE']
tSUPPORTED_DATA_FEEDS = Literal['YAHOO_FINANCE', 'BYBIT']
tSUPPORTED_STORAGES = Literal['local', 'minio']
tSUPPORTED_DOWNLOAD_DATA_SOURCES = Literal['BYBIT', 'BINANCE']
tSUPPORTED_CRYPTO_EXCHANGES = Literal['BYBIT', 'BINANCE']
tSUPPORTED_DATA_TOOLS = Literal['pandas', 'polars', 'dask', 'spark']
tSUPPORTED_DATA_ENGINES = Literal['ray', 'dask', 'spark']
tSUPPORTED_DATA_TYPES = Literal[
    'raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily',
    'tick', 'second', 'minute', 'hour', 'daily'
]