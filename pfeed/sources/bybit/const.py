"""Metadata of bybit's data"""
DATA_SOURCE = 'BYBIT'
SUPPORTED_PRODUCT_TYPES = ['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
SUPPORTED_RAW_DATA_TYPES = ['raw_tick']
DATA_START_DATE = '2020-01-01'  # do not need to be precise, if it doesn't exist, it will be skipped
DATA_SOURCE_URLS = {
    'PERP': 'https://public.bybit.com/trading',
    'FUT': 'https://public.bybit.com/trading',
    'IPERP': 'https://public.bybit.com/trading',
    'IFUT': 'https://public.bybit.com/trading',
    'SPOT': 'https://public.bybit.com/spot',
}
DATA_NAMING_REGEX_PATTERNS = {
    'PERP': '(USDT\/|PERP\/)$',  # USDT perp or USDC perp;
    'FUT': '-\d{2}[A-Z]{3}\d{2}\/$',  # USDC futures e.g. BTC-10NOV23/
    'IPERP': 'USD\/$',  # inverse perps;
    'IFUT': 'USD[A-Z]\d{2}\/$',  # inverse futures e.g. BTCUSDH24/
    # match everything since everything from https://public.bybit.com/spot is spot
    'SPOT': '.*',
}
MAPPING_COLS = {'Buy': 1, 'Sell': -1}
RENAMING_COLS = {'timestamp': 'ts', 'size': 'volume'}