from typing import Literal


tDATA_SOURCE = Literal['YAHOO_FINANCE', 'DATABENTO', 'BYBIT']
tPRODUCT_TYPE = Literal[
    'STK', 'FUT', 'ETF', 'OPT', 'FX', 'CRYPTO', 'BOND', 'MTF', 'CMDTY', 'INDEX',
    'PERP', 'IPERP', 'SPOT', 'IFUT'
]
tSTORAGE = Literal['cache', 'local', 'minio', 'duckdb']
tDATA_TOOL = Literal['pandas', 'polars', 'dask']
tENVIRONMENT = Literal['BACKTEST', 'SANDBOX', 'PAPER', 'LIVE']