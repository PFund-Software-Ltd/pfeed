SUPPORTED_DATA_FEEDS = ['YAHOO_FINANCE', 'BYBIT']
SUPPORTED_DATA_TYPES = ['raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily', 
                        'tick', 'second', 'minute', 'hour', 'daily']
SUPPORTED_DATA_STORAGES = ['local', 'minio']
SUPPORTED_DOWNLOAD_DATA_SOURCES = ['BYBIT']
ALIASES = {
    'YF': 'YAHOO_FINANCE',
}
SUPPORTED_DATA_MODES = ['historical', 'streaming']