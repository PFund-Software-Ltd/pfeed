from typing import Any

import datetime
from pathlib import Path

import pytest
import polars as pl
from pfund.datas.resolution import Resolution

import pfeed as pe
from pfeed.dataflow.result import RunResult


@pytest.mark.parametrize(('product', 'resolution'), [
    ('BTC_USDT_PERP', '1t'),  # USDT perpetual
    ('BTC_USDT_FUT', '1s'),  # USDT future
    ('BTC_USDC_PERPETUAL', '1m'),  # USDC perpetual
    ('BTC_USDC_FUTURE', '1h'),  # USDC future
    ('BTC_USD_IPERP', '1d'),  # inverse perpetual
    ('BTC_USD_INVERSE-FUTURE', '1t'),  # inverse future
    ('BTC_USDC_SPOT', '1t'),  # spot
])
def test_download_and_retrieve(tmp_path: Path, bybit: pe.Bybit, product: str, resolution: str):
    def _assert_df(df: pl.DataFrame, start_date: str, end_date: str) -> None:
        assert df is not None
        assert df.height > 0
        _resolution = Resolution(resolution)
        assert df.columns[:3] == ['date', 'product', 'resolution']
        if _resolution.is_bar():
            assert df.columns == ['date', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume', 'n_data_points']
        elif _resolution.is_tick():
            # vendor columns are kept, only core columns are guaranteed
            assert {'date', 'product', 'resolution', 'symbol', 'side', 'volume', 'price'} <= set(df.columns)
        elif _resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        assert df['date'].is_sorted()
        if _resolution.is_bar():
            assert df['date'].is_unique().all()  # sorted + unique = strictly increasing
        assert df['resolution'].n_unique() == 1
        assert df['product'].n_unique() == 1
        assert df['resolution'][0] == str(_resolution)
        # crypto trades 24/7, so every day in [start_date, end_date] should have data
        expected_dates = pl.date_range(
            datetime.date.fromisoformat(start_date),
            datetime.date.fromisoformat(end_date),
            eager=True,
        ).to_list()
        assert df['date'].dt.date().unique().sort().to_list() == expected_dates

    start_date, end_date = '2026-09-01', '2026-09-02'
    expiration = '2026-09-25'
    feed = bybit.market_feed
    is_future = 'FUT' in product
    if is_future:
        product_specs: dict[str, Any] = {'expiration': expiration}
    else:
        product_specs: dict[str, Any] = {}
    result = feed.download(
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
        **product_specs
    )
    assert isinstance(result, RunResult)
    assert result.success
    data = result.data
    assert isinstance(data, pl.LazyFrame)
    df = data.collect()
    _assert_df(df, start_date, end_date)
#     df = feed.retrieve(
#         product=product,
#         resolution=resolution,
#         start_date=start_date,
#         end_date=end_date,
#         **product_specs
#     )
#     _assert_df(df, start_date, end_date)


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
def test_pipeline_mode(tmp_path, bybit):
    pass
