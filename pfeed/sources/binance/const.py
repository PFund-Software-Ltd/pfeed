"""Metadata of binance's data
Examples of the market data file's name of different products types in binance vision:
    PERP: BTCUSDT-trades-2024-04-09.zip
    FUT: BTCUSDT_240927-trades-2024-04-06.zip
    IPERP: BTCUSD_PERP-trades-2024-04-08.zip
    IFUT: BTCUSD_240927-trades-2024-04-09.zip
    SPOT: BTCUSDT-trades-2024-04-06.zip  
"""
from pfund.exchanges.binance.exchange import Exchange
PTYPE_TO_CATEGORY = Exchange.PTYPE_TO_CATEGORY

DATA_SOURCE = 'BINANCE'
SUPPORTED_PRODUCT_TYPES = ['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT']
SUPPORTED_RAW_DATA_TYPES = ['raw_tick']
DATA_START_DATE = '2020-01-01'  # do not need to be precise, if it doesn't exist, it will be skipped
DATA_SOURCE_URLS = {
    'PERP': 'https://data.binance.vision/data/futures/um/daily/trades',
    'FUT': 'https://data.binance.vision/data/futures/um/daily/trades',
    'IPERP': 'https://data.binance.vision/data/futures/cm/daily/trades',
    'IFUT': 'https://data.binance.vision/data/futures/cm/daily/trades',
    'SPOT': 'https://data.binance.vision/data/spot/daily/trades',
}
DATA_NAMING_REGEX_PATTERNS = {
    # USDT/USDC/BUSD perp, BTC perp, e.g. BTCUSDT/ , BTCUSDC/ , ETHBTC/
    'PERP': '(USDT\/|USDC\/|BUSD\/|BTC\/)$',
    'FUT': '\d{6}\/$',  # USDT futures e.g. BTCUSDT_240927/
    'IPERP': 'PERP\/$',  # inverse perps e.g. BTCUSD_PERP/
    'IFUT': '\d{6}\/$',  # inverse futures e.g. ETHUSD_240628/
    # match everything since everything from https://data.binance.vision/?prefix=data/spot/daily/trades/ is spot
    'SPOT': '.*',
}
# TODO:
MAPPING_COLS = {'Buy': 1, 'Sell': -1}
# TODO:
RENAMING_COLS = {'timestamp': 'ts', 'size': 'volume'}

# TODO: support other ptypes
SUPPORTED_PRODUCT_TYPES = ['PERP']
adapters = {ptype: Exchange(env='LIVE', ptype=ptype).adapter for ptype in SUPPORTED_PRODUCT_TYPES}


def create_efilename(pdt: str, date: str):
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    adapter = adapters[ptype]
    epdt = adapter(pdt, ref_key=PTYPE_TO_CATEGORY[ptype])
    return f'{epdt}-trades-{date}.zip'
 