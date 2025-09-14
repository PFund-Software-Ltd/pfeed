import time

import pytest

import pfeed as pe
from pfeed._etl.base import convert_to_pandas_df


@pytest.mark.parametrize(('product', 'resolution'), [('AAPL_USD_STK', '1d')])
@pytest.mark.rate_limit
def test_download_and_retrieve(tmp_path, yahoo_finance, product, resolution):
    def _assert_df(df):
        df = convert_to_pandas_df(df)
        assert df.columns.tolist() == ['date', 'product', 'resolution', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'splits']
        is_strictly_increasing = df["date"].is_monotonic_increasing and df["date"].is_unique
        assert is_strictly_increasing is True
        assert df['resolution'].nunique() == 1
        assert df['symbol'].nunique() == 1
        assert df['product'].nunique() == 1
        assert df['resolution'].iloc[0] == resolution
        assert len(df) >= 1  # or > 0 to ensure we got data
    pe.configure(data_path=tmp_path / 'data')
    start_date, end_date = '2025-08-01', '2025-08-07'
    feed = yahoo_finance.market_feed
    df = feed.download(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
    )
    _assert_df(df)
    time.sleep(1)
    # NOTE: test retrieve() after download() due to dependency on downloaded data in tmp_path
    df = feed.retrieve(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
    )
    _assert_df(df)


@pytest.mark.parametrize(('product', 'resolution'), [('ES_USD_FUTURE', '1h')])
@pytest.mark.rate_limit
def test_download_and_retrieve_mixed(tmp_path, yahoo_finance_mixed, product, resolution):
    def _assert_df(df):
        df = convert_to_pandas_df(df)
        assert set(['Open', 'High', 'Low', 'Close', 'Volume']).issubset(df.columns.tolist())
        assert len(df) >= 1  # or > 0 to ensure we got data
    pe.configure(data_path=tmp_path / 'data')
    start_date, end_date = '2025-08-07', '2025-08-07'
    auto_transform = False  # raw df from yfinance, not normalized
    product_specs = {}
    is_futures = product.endswith('_FUTURE')
    if is_futures:
        # NOTE: this doesn't need to be accurate, "expiration" is supposed to be a field provided by user for their own reference.
        # e.g. for ES_USD_FUTURE, even its symbol is ES=F (Sept 25), we can just set the expiration to be 2025-08-01, 
        # and a wrong symbol will be derived internally, in this case, ESQ25 where Q = August
        product_specs['expiration'] = '2025-08-07'
    feed = yahoo_finance_mixed.market_feed
    df = feed.download(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
        auto_transform=auto_transform,
        **product_specs
    )
    _assert_df(df)
    time.sleep(3)
    df = feed.retrieve(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
        auto_transform=auto_transform,
        **product_specs
    )
    _assert_df(df)
    
    
def test_stream_and_retrieve(tmp_path, yahoo_finance):
    pass


def test_stream_and_retrieve_mixed(tmp_path, yahoo_finance_mixed):
    pass