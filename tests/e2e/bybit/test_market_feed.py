import pytest

import pfeed as pe
from pfund.datas.resolution import Resolution
from pfeed._etl.base import convert_to_desired_df
from pfeed.enums import DataTool


@pytest.mark.parametrize(('product', 'resolution'), [
    ('BTC_USDT_PERP', '1t'),  # USDT perpetual
    ('BTC_USDT_FUT', '1s'),  # USDT future
    ('BTC_USDC_PERPETUAL', '1m'),  # USDC perpetual
    ('BTC_USDC_FUTURE', '1h'),  # USDC future
    ('BTC_USD_IPERP', '1d'),  # inverse perpetual
    ('BTC_USD_INVERSE-FUTURE', '1t'),  # inverse future
    ('BTC_USDC_SPOT', '1t'),  # spot
])
def test_download_and_retrieve(tmp_path, bybit, product, resolution):
    import datetime

    def _assert_df(df, start_date, end_date):
        assert df is not None
        df = convert_to_desired_df(df, DataTool.pandas)
        _resolution = Resolution(resolution)
        if _resolution.is_bar():
            assert df.columns.tolist() == ['date', 'product', 'resolution', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        elif _resolution.is_tick():
            assert df.columns.tolist() == ['date', 'product', 'resolution', 'symbol', 'side', 'volume', 'price']
        # TODO:
        elif _resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        is_monotonic_increasing = df["date"].is_monotonic_increasing
        assert is_monotonic_increasing is True
        if _resolution.is_bar():
            is_strictly_increasing = is_monotonic_increasing and df["date"].is_unique
            assert is_strictly_increasing is True
        assert df['resolution'].nunique() == 1
        assert df['symbol'].nunique() == 1
        assert df['product'].nunique() == 1
        assert df['resolution'].iloc[0] == resolution
        assert len(df) >= 1  # or > 0 to ensure we got data
        # Check date range: crypto trades 24/7, so first and last dates should match start/end dates
        first_date = df['date'].iloc[0].date() if hasattr(df['date'].iloc[0], 'date') else df['date'].iloc[0]
        last_date = df['date'].iloc[-1].date() if hasattr(df['date'].iloc[-1], 'date') else df['date'].iloc[-1]
        expected_start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        expected_end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        assert first_date == expected_start, f"First date {first_date} doesn't match start_date {expected_start}"
        assert last_date == expected_end, f"Last date {last_date} doesn't match end_date {expected_end}"

    pe.configure(data_path=tmp_path / 'data')
    start_date, end_date = '2025-09-01', '2025-09-02'
    expiration = '2025-09-26'
    feed = bybit.market_feed
    is_future = 'FUT' in product
    if is_future:
        product_specs = {'expiration': expiration}
    else:
        product_specs = {}
    df = feed.download(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
        **product_specs
    )
    _assert_df(df, start_date, end_date)
    df = feed.retrieve(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
        **product_specs
    )
    _assert_df(df, start_date, end_date)
    

@pytest.mark.parametrize(('product', 'resolution'), [('ETH_USDT_PERP', '1t')])
def test_download_and_retrieve_mixed(tmp_path, bybit_mixed, product, resolution):
    
    def _assert_df(df, start_date, end_date):
        assert df is not None
        df = convert_to_desired_df(df, DataTool.pandas)
        print(df.head())
        
    pe.configure(data_path=tmp_path / 'data')
    start_date, end_date = '2025-09-01', '2025-09-02'
    auto_transform = False  # raw df from bybit, not normalized
    feed = bybit_mixed.market_feed
    df = feed.download(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
        auto_transform=auto_transform,
    )
    _assert_df(df, start_date, end_date)
    # df = feed.retrieve(
    #     product=product,
    #     resolution=resolution,
    #     start_date=start_date,
    #     end_date=end_date,
    #     auto_transform=auto_transform,
    # )
    # _assert_df(df, start_date, end_date)


# TODO
@pytest.mark.parametrize(('product', 'resolution'), [
    ('HYPE_USDT_PERP', '1q_L2'),
    ('XRP_USDT_CRYPTO', '10q_L1'),
    ('SOL_USD_IPERP', '1m'),
])
def test_stream_and_retrieve(tmp_path, bybit, product, resolution):
    def _callback(msg):
        print(msg)
    bybit.stream(product=product, resolution=resolution, callback=_callback)


# TODO
@pytest.mark.parametrize(('product', 'resolution'), [('BTC_USDT_PERP', '1q_L2')])
@pytest.mark.asyncio
async def test_stream_and_retrieve_async(tmp_path, bybit, product, resolution):
    async for msg in bybit.stream(product=product, resolution=resolution):
        print(msg)


# TODO
def test_stream_and_retrieve_mixed(tmp_path, bybit_mixed):
    pass


# TODO
def test_pipeline_mode(tmp_path, bybit):
    pass