[MinIO]: https://min.io/

# Quickstart

There are mainly 4 ways to get data in pfeed:
1. `get_historical_data()` stores the data in memory and returns it as a DataFrame/LazyFrame.
If the data has already been downloaded, it will be loaded.
```python
import pfeed as pe
feed = pe.BybitFeed(data_tool='pandas', debug=True)
df = feed.get_historical_data('BTC_USDT_PERP', rollback_period='1w', resolution='1d')
```
2. `get_realtime_data()` gets real-time data by calling the broker/exchange's API.
```python
# TODO: Not supported yet
data: dict = feed.get_realtime_data(...)
```
3. `download_historical_data()` or `download()` loads the data into your local machine, local [MinIO] or the cloud.
```python
pe.download('bybit', pdts=['BTC_USDT_PERP'], dtypes=['minute'], start_date='2024-01-01', end_date='2024-01-03', use_ray=True, use_minio=False, debug=True)
```
4. `stream_realtime_data()` or `stream()` listens to real-time data via websocket and stores it optionally.
```python
# TODO: Not supported yet
pe.stream(...)
```


## Table of Contents
```{tableofcontents}
```