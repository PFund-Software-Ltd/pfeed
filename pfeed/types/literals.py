from typing import Literal


tDATA_SOURCE = Literal['yahoo_finance', 'databento', 'bybit', 'binance']
tENVIRONMENT = Literal['backtest', 'sandbox', 'paper', 'live']
tSTORAGE = Literal['cache', 'local', 'minio']
tDATA_TOOL = Literal['pandas', 'polars', 'dask', 'spark']
tMARKET_DATA_TYPE = Literal[
    'quote_l1', 'quote_l2', 'quote_l3', 'tick', 
    'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
]
# EXTEND
tDATA_TYPE = Literal[
    'quote_l1', 'quote_l2', 'quote_l3', 'tick', 
    'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
]
tPRODUCT_TYPE = Literal[
    'STK', 'FUT', 'ETF', 'OPT', 'FX', 'CRYPTO', 'BOND', 'MTF', 'CMDTY', 
    'PERP', 'IPERP', 'SPOT', 'IFUT'
]