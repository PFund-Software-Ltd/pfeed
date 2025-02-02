[MinIO]: https://min.io/
[Delta Lake]: https://delta-io.github.io/delta-rs/

# Quickstart

There are mainly 6 ways to interact with data in pfeed:
1. `get_historical_data()` gets historical data from local storage (if exists) or downloads from remote data sources and stores in cache, then returns a cleaned DataFrame/LazyFrame.
```{code-block} python
import pfeed as pe
feed = pe.BybitFeed(data_tool='polars', use_ray=True)
df = feed.get_historical_data('BTC_USDT_PERP', rollback_period='1w', resolution='1d')
```
2. `download()` downloads data into your local machine or the cloud, supports [Delta Lake] format.
```{code-block} python
feed.download(
    product='BTC_USDT_PERP', 
    resolution='minute', 
    start_date='2025-01-01', 
    end_date='2025-01-03', 
    to_storage='local',  # or 'minio', if MinIO is running
)
```
3. 🚧 `get_realtime_data()` gets real-time data by calling the broker/exchange's API.
```{code-block} python
data: dict = feed.get_realtime_data(...)
```
4. 🚧 `stream_realtime_data()` or `stream()` listens to real-time data via websocket and optionally stores it .
```{code-block} python
pe.stream(...)
```


```{hint}
You can run the command "**pfeed config set --data-path {your_path}**" to specify the path where downloaded data will be stored.
```
