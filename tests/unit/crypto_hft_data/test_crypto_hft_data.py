from __future__ import annotations

import pandas as pd
import polars as pl
from pfund.datas.resolution import Resolution

import pfeed as pe
from pfeed.enums import DataSource
from pfeed.sources.crypto_hft_data.market_feed import CryptoHftDataMarketFeed
from pfeed.sources.crypto_hft_data.market_data_model import (
    CryptoHftDataMarketDataModel,
)
from pfeed.sources.crypto_hft_data.source import CryptoHftDataSource


class FakeCryptoHftDataClient:
    def __init__(self, trades: pd.DataFrame | None = None):
        self.trades = trades
        self.calls: list[dict[str, str]] = []

    def get_trades(self, **kwargs: str) -> pd.DataFrame:
        self.calls.append(kwargs)
        assert self.trades is not None
        return self.trades

    def list_exchanges(self) -> list[str]:
        return ["BINANCE", "BYBIT"]

    def list_symbols(self, exchange: str, data_type: str | None = None) -> list[str]:
        return [f"{exchange}:BTCUSDT:{data_type or 'all'}"]


def make_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "received_time": [1_720_656_000_100_000_000, 1_720_656_000_200_000_000],
            "event_time": [1_720_656_000_100, 1_720_656_000_200],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "trade_id": [1, 2],
            "price": [60_000.0, 60_001.0],
            "quantity": [0.1, 0.2],
            "trade_time": [1_720_656_000_100, 1_720_656_000_200],
            "is_buyer_maker": [False, True],
            "order_type": ["market", "market"],
        }
    )


def test_public_client_aliases_and_discovery() -> None:
    client = pe.CryptoHFTData()
    fake_api = FakeCryptoHftDataClient()
    client.data_source._batch_api = fake_api

    assert pe.CHD is pe.CryptoHFTData
    assert client.name == DataSource.CRYPTO_HFT_DATA
    assert client.list_exchanges() == ["BINANCE", "BYBIT"]
    assert client.list_symbols("BINANCE", "trades") == ["BINANCE:BTCUSDT:trades"]


def test_api_key_environment_variable_precedence(monkeypatch) -> None:
    monkeypatch.setenv("CHD_API_KEY", "alias-key")
    monkeypatch.setenv("CRYPTO_HFT_DATA_API_KEY", "enum-key")
    monkeypatch.setenv("CRYPTOHFTDATA_API_KEY", "official-key")

    assert CryptoHftDataSource()._api_key == "official-key"


def test_products_are_exchange_aware() -> None:
    source = CryptoHftDataSource()
    binance = source.create_product("BTC_USDT_PERP", exchange="BINANCE")
    bybit = source.create_product("BTC_USDT_PERP", exchange="BYBIT")

    assert binance.symbol == "BTCUSDT"
    assert binance.name == "CRYPTO_HFT_DATA_BINANCE_BTCUSDT"
    assert binance != bybit
    assert len({binance, bybit}) == 2


def test_normalize_raw_trades() -> None:
    raw = pl.from_pandas(make_trades()).lazy()

    normalized = CryptoHftDataMarketFeed._normalize_raw_data(raw).collect()

    assert normalized["volume"].to_list() == [0.1, 0.2]
    assert normalized["side"].to_list() == [1, -1]
    assert normalized.schema["price"] == pl.Float64
    assert normalized.schema["volume"] == pl.Float64
    assert normalized.schema["side"] == pl.Int8


def test_download_impl_uses_exchange_and_symbol() -> None:
    feed = CryptoHftDataMarketFeed()
    fake_api = FakeCryptoHftDataClient(make_trades())
    feed.data_source._batch_api = fake_api
    model = feed.create_data_model(
        product="BTC_USDT_PERP",
        exchange="BINANCE",
        symbol="BTCUSDT",
        resolution="tick",
        start_date="2024-07-13",
        data_origin="binance",
    )
    assert isinstance(model, CryptoHftDataMarketDataModel)

    result = feed._download_impl(model, Resolution(model.resolution))

    assert result is not None
    assert result.collect().height == 2
    assert fake_api.calls == [
        {
            "symbol": "BTCUSDT",
            "exchange": "BINANCE",
            "start_date": "2024-07-13",
            "end_date": "2024-07-13",
        }
    ]
