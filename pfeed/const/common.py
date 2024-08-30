SUPPORTED_DATA_FEEDS = ['YAHOO_FINANCE', 'BYBIT', 'BINANCE']
SUPPORTED_DATA_TYPES = ['raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily', 
                        'tick', 'second', 'minute', 'hour', 'daily']
SUPPORTED_DATA_SINKS = ['local', 'minio']
SUPPORTED_DOWNLOAD_DATA_SOURCES = ['BYBIT', 'BINANCE']
SUPPORTED_DATA_MODES = ['historical', 'streaming']
SUPPORTED_DATA_TOOLS = ['pandas', 'polars']

ALIASES = {
    'YF': 'YAHOO_FINANCE',
}