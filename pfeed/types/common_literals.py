from typing import Literal

# since Literal doesn't support variables as inputs, define variables in common.py here with prefix 't'
tSUPPORTED_DATA_FEEDS = Literal['YAHOO_FINANCE', 'BYBIT']
tSUPPORTED_DATA_TYPES = Literal['raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily', 
                                'tick', 'second', 'minute', 'hour', 'daily']
tSUPPORTED_DATA_SINKS = Literal['local', 'minio']
tSUPPORTED_DOWNLOAD_DATA_SOURCES = Literal['BYBIT']
tSUPPORTED_DATA_MODES = Literal['historical', 'streaming']
tSUPPORTED_DATA_TOOLS = Literal['pandas', 'polars']