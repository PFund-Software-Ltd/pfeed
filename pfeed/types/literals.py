from typing import Literal


tDATA_SOURCE = Literal['YAHOO_FINANCE', 'DATABENTO', 'BYBIT', 'BINANCE']
tPRODUCT_TYPE = Literal[
    'STK', 'FUT', 'ETF', 'OPT', 'FX', 'CRYPTO', 'BOND', 'MTF', 'CMDTY', 
    'PERP', 'IPERP', 'SPOT', 'IFUT'
]
tSTORAGE = Literal['cache', 'local', 'minio']
tDATA_TOOL = Literal['pandas', 'polars', 'dask', 'spark']