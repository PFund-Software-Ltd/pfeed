DATA_SOURCE = 'BYBIT'
# EXTEND: support FUT and IFUT
# SUPPORTED_CRYPTO_PRODUCT_TYPES = ['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
SUPPORTED_CRYPTO_PRODUCT_TYPES = ['SPOT', 'PERP', 'IPERP']
RAW_DATA_TYPE = 'tick'
# do not need to be precise
DATA_START_DATE = '2020-01-01'
DATA_SOURCE_URLS = {
    'linear': 'https://public.bybit.com/trading',
    'inverse': 'https://public.bybit.com/trading',
    'spot': 'https://public.bybit.com/spot',
}
DATA_NAMING_REGEX_PATTERNS = {
    'PERP': '(USDT\/|PERP\/)$',  # USDT perp or USDC perp;
    'FUT': '-\d{2}[A-Z]{3}\d{2}\/$',  # USDC futures e.g. BTC-10NOV23/
    'IPERP': 'USD\/$',  # inverse perps;
    'IFUT': 'USD[A-Z]\d{2}\/$',  # inverse futures e.g. BTCUSDH24/
    # match everything since everything from https://public.bybit.com/spot is spot
    'SPOT': '.*',
}
SELECTED_RAW_COLS = {
    'linear': ['timestamp', 'side', 'size', 'price'],
    'inverse': ['timestamp', 'side', 'size', 'price'],
    'spot': ['timestamp', 'side', 'volume', 'price'],
}
RENAMING_COLS = {
    'linear': {'timestamp': 'ts', 'size': 'volume'},
    'inverse': {'timestamp': 'ts', 'size': 'volume'},
    'spot': {'timestamp': 'ts'},
}
RAW_DATA_TIMESTAMP_UNITS = {
    'linear': 's',
    'inverse': 's',
    'spot': 'ms'
}


def create_efilename(epdt, date, is_spot=False):
    if is_spot:
        return f'{epdt}_{date}.csv.gz'
    else:
        return f'{epdt}{date}.csv.gz'