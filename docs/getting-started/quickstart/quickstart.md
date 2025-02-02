# Quickstart

There are mainly **6 ways** to interact with data in pfeed:

1. *`get_historical_data()`* retrieves stored data from storage (if any) or downloads it from data sources. The data is then cached and returned as a cleaned DataFrame or LazyFrame.
```{code-block} python
import pfeed as pe
feed = pe.BybitFeed(data_tool='polars', use_ray=True)
df = feed.get_historical_data('BTC_USDT_PERP', rollback_period='1w', resolution='1d')
```
---
2. *`download()`* saves data locally or to the cloud.
```{code-block} python
df = feed.download(
    product='BTC_USDT_PERP', 
    resolution='1m',
    start_date='2025-01-01', 
    end_date='2025-01-03', 
    to_storage='local',  # or 'minio', if MinIO is running
)
```
---
3. *`retrieve()`* loads previously stored data from local or cloud storage.
```{code-block} python
df = feed.retrieve(
    product='BTC_USDT_PERP', 
    resolution='1m',
    start_date='2025-01-01', 
    end_date='2025-01-03', 
)
```
---
4. 🚧 *`get_realtime_data()`* fetches real-time data directly from the broker/exchange's API.
---
5. 🚧 *`stream()`* listens to real-time data via websocket and can optionally store it .
---
6. 🚧 *`fetch()`* pulls data from a data source's API without storing it.
