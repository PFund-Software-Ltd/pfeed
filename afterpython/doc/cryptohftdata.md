# CryptoHFTData

[CryptoHFTData](https://cryptohftdata.com) provides historical trades and order
book updates from multiple cryptocurrency exchanges. PFeed's integration treats
the upstream exchange as part of each product's identity, so identically named
symbols from different venues remain separate throughout download and storage.

## Install

```bash
pip install "pfeed[cryptohftdata]"
```

The free tier works without credentials. For higher account limits, set one of
the supported environment variables before creating the client:

```bash
export CRYPTOHFTDATA_API_KEY="your-api-key"
```

`CRYPTO_HFT_DATA_API_KEY` and `CHD_API_KEY` are also recognized.

## Discover exchanges and symbols

```python
import pfeed as pe

client = pe.CryptoHFTData()
print(client.list_exchanges())
print(client.list_symbols("binance_futures", data_type="trades"))
```

## Download trades or bars

```python
client = pe.CryptoHFTData()
result = client.market_feed.download(
    product="BTC_USDT_PERP",
    exchange="binance_futures",
    symbol="BTCUSDT",
    resolution="tick",  # use "1m", "1h", etc. for resampled OHLCV bars
    start_date="2024-07-13",
    end_date="2024-07-13",
)
trades = result.data.collect()
```

Pass the exchange identifier exactly as CryptoHFTData returns it. Passing an
explicit symbol is recommended because exchange naming conventions can differ;
when omitted, PFeed derives a compact base/quote symbol such as `BTCUSDT`.

For persistent downloads, pass a normal `StorageConfig`. PFeed uses the exchange
as `data_origin`, preventing Binance and Bybit data for the same product from
sharing a storage partition.

## Current scope

The integration supports historical trade ticks and all coarser bar resolutions
that PFeed can derive from them. CryptoHFTData also offers historical order book
data, but PFeed's quote/order-book validation and resampling pipeline is not yet
implemented. Order books are therefore not exposed here until PFeed can preserve
their semantics end to end. CryptoHFTData does not provide a live-streaming feed.

See the runnable [example script](../../examples/cryptohftdata.py) and
[example notebook](../example/cryptohftdata.ipynb).
