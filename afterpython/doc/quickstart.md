# Quickstart

> Want to try first? Run `pfeed` in your browser — no install needed — at
 **[pfeed.pfund.ai](https://pfeed.pfund.ai/)**.

```python
import pfeed as pe

bybit = pe.Bybit()
feed = bybit.market_feed
```
---

### 1. Download Historical Data
```python
result = feed.download(
    product="BTC_USDT_PERP",  # or BTC_USDT_PERPETUAL in full
    resolution="1minute",  # '1tick'/'1t' or '2second'/'2s' etc.
    start_date="2026-01-01",
    end_date="2026-01-01",
    io=pe.ParquetIO(),  # None to skip storing
)
df = result.data.collect()
```
---

### 2. Retrieve Stored Data
```python
result = feed.retrieve(
    product="BTC_USDT_PERP",
    resolution="1m",  # 1-minute data
    start_date="2026-01-01",
    end_date="2026-01-01",
    io=pe.ParquetIO(),
)
df = result.data.collect()
```
---

### 3. Stream Live Data
```python
feed.stream(
    product="BTC_USDT_CRYPTO",
    resolution="1tick",  # or just 'tick' for tick data
    callback=lambda msg: print(msg),
)
```
---

```{tip} Learn More
- For more advanced usage, see the [Tutorials](https://pfeed.pfund.ai/tutorial).
- For how to write a `product` and `resolution`, see [Product](https://pfund.pfund.ai/doc/product) and [Resolution](https://pfund.pfund.ai/doc/resolution) in the `pfund` docs.
```
