from functools import lru_cache


@lru_cache(maxsize=1)
def get_exchange():
    from pfund.exchanges.bybit.exchange import Exchange
    return Exchange(env='LIVE')


def create_efilename(pdt: str, date: str):
    ptype = pdt.split('_')[-1].upper()  # REVIEW: is this always the case?
    exchange = get_exchange()
    category = exchange.PTYPE_TO_CATEGORY[ptype]
    epdt = exchange.adapter(pdt, ref_key=category)
    is_spot = (ptype == 'SPOT')
    if is_spot:
        return f'{epdt}_{date}.csv.gz'
    else:
        return f'{epdt}{date}.csv.gz'