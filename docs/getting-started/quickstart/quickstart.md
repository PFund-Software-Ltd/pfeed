[MinIO]: https://min.io/

# Quickstart

There are mainly 4 ways to interact with data in pfeed:
1. `get_historical_data()` gets historical data from local storage (if exists) or remote data sources and returns a cleaned DataFrame/LazyFrame.
```{code-block} python
import pfeed as pe
feed = pe.BybitFeed(data_tool='pandas')
df = feed.get_historical_data('BTC_USDT_PERP', rollback_period='1w', resolution='1d')
```
2. `download_historical_data()` or `download()` loads the downloaded data into your local machine, local data lake [MinIO], or the cloud.
```{code-block} python
pe.download('bybit', products=['BTC_USDT_PERP'], dtypes=['minute'], start_date='2024-01-01', end_date='2024-01-03', use_ray=True, use_minio=False)
```
3. 🚧 `get_realtime_data()` gets real-time data by calling the broker/exchange's API.
```{code-block} python
data: dict = feed.get_realtime_data(...)
```
4. 🚧 `stream_realtime_data()` or `stream()` listens to real-time data via websocket and optionally stores it .
```{code-block} python
pe.stream(...)
```