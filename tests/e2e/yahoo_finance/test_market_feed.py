import time

import pytest

import pfeed as pe
from pfeed._etl.base import convert_to_pandas_df


@pytest.mark.parametrize(
    ('product', 'resolution'), 
    [
        ('AAPL_USD_STK', '1d'),
        # ('ES_USD_FUTURE', '1h'),
    ]
)
def test_download(yahoo_finance, product, resolution):
    feed = yahoo_finance.market_feed
    df = feed.download(
        product=product,
        resolution=resolution,
        start_date='2025-08-01',
        end_date='2025-08-08',
    )
    df = convert_to_pandas_df(df)
    print(df)
    time.sleep(1)