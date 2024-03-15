"""Metadata of bybit's data"""
from pfund.exchanges.bybit.exchange import Exchange
PTYPE_TO_CATEGORY = Exchange.PTYPE_TO_CATEGORY

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
SELECTED_RAW_COLS = {
    'PERP': ['timestamp', 'side', 'size', 'price'],
    'FUT': ['timestamp', 'side', 'size', 'price'],
    'IPERP': ['timestamp', 'side', 'size', 'price'],
    'IFUT': ['timestamp', 'side', 'size', 'price'],
    'SPOT': ['timestamp', 'side', 'volume', 'price'],
}
RENAMING_COLS = {
    'PERP': {'timestamp': 'ts', 'size': 'volume'},
    'FUT': {'timestamp': 'ts', 'size': 'volume'},
    'IPERP': {'timestamp': 'ts', 'size': 'volume'},
    'IFUT': {'timestamp': 'ts', 'size': 'volume'},
    'SPOT': {'timestamp': 'ts'},
}
RAW_DATA_TIMESTAMP_UNITS = {
    'PERP': 's',
    'FUT': 's',
    'IPERP': 's',
    'IFUT': 's',
    'SPOT': 'ms'
}


def create_efilename(pdt: str, date: str):
    adapter = Exchange(env='LIVE').adapter
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    epdt = adapter(pdt, ref_key=PTYPE_TO_CATEGORY[ptype])
    is_spot = (ptype == 'SPOT')
    if is_spot:
        return f'{epdt}_{date}.csv.gz'
    else:
        return f'{epdt}{date}.csv.gz'
    
    