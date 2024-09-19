from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    import pandas as pd
    from pfeed.resolution import ExtendedResolution
    
    
from functools import lru_cache


@lru_cache(maxsize=1)
def get_exchange():
    from pfund.exchanges.bybit.exchange import Exchange
    return Exchange(env='LIVE')


def create_efilename(pdt: str, date: str | datetime.date):
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    exchange = get_exchange()
    category = exchange.PTYPE_TO_CATEGORY[ptype]
    epdt = exchange.adapter(pdt, ref_key=category)
    is_spot = (ptype == 'SPOT')
    if is_spot:
        return f'{epdt}_{date}.csv.gz'
    else:
        return f'{epdt}{date}.csv.gz'


def get_default_raw_resolution() -> ExtendedResolution:
    from pfeed.resolution import ExtendedResolution
    from pfeed.sources.bybit.const import SUPPORTED_DATA_TYPES, DTYPES_TO_RAW_RESOLUTIOS
    return ExtendedResolution(DTYPES_TO_RAW_RESOLUTIOS[SUPPORTED_DATA_TYPES[0]])


def standardize_ts_column(df: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd
    # NOTE: for ptype SPOT, unit is 'ms', e.g. 1671580800123, in milliseconds
    unit = 'ms' if df['ts'][0] > 10**12 else 's'  # REVIEW
    # NOTE: somehow some data is in reverse order, e.g. BTC_USDT_PERP in 2020-03-25
    is_in_reverse_order = df['ts'][0] > df['ts'][1]
    if is_in_reverse_order:
        df['ts'] = df['ts'][::-1].values
    # NOTE: this may make the `ts` value inaccurate, e.g. 1671580800.9906 -> 1671580800.990600192
    df['ts'] = pd.to_datetime(df['ts'], unit=unit)
    return df