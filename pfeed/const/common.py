SUPPORTED_ENVIRONMENTS = ['BACKTEST', 'SANDBOX', 'PAPER', 'LIVE']
SUPPORTED_DATA_FEEDS = ['YAHOO_FINANCE', 'BYBIT', 'BINANCE']
SUPPORTED_STORAGES = ['local', 'minio']
SUPPORTED_DOWNLOAD_DATA_SOURCES = ['BYBIT', 'BINANCE']
SUPPORTED_CRYPTO_EXCHANGES = ['BYBIT', 'BINANCE']
SUPPORTED_DATA_TOOLS = ['pandas', 'polars']
SUPPORTED_PRODUCT_TYPES = ['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
SUPPORTED_DATA_TYPES = [
    'raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily',
    'tick', 'second', 'minute', 'hour', 'daily',
]

ALIASES = {
    'YF': 'YAHOO_FINANCE',
}