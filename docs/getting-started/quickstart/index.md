# Quickstart

There are mainly 4 ways to get data in pfeed:
1. `get_historical_data()` stores the data in memory, return it as a DataFrame/LazyFrame. 
If the data is already downloaded, it will be loaded from the local storage. If not, it will be downloaded on the fly.
```python
import pfeed as pe
feed = pe.BybitFeed()
df = feed.get_historical_data(...)
```
2. `get_realtime_data()` (**TODO: Not supported yet**) gets real-time data by calling the broker's/exchange's API.
```python
data: dict = feed.get_realtime_data(...)
```
3. `download_historical_data()` loads the data into your local machine, or your local [MinIO](https://min.io/)
```python
pe.bybit.download_historical_data(...)
# or for short:
pe.bybit.download(...)
```
4. `stream_realtime_data()` (**TODO: Not supported yet**) listens to real-time data via websocket and stores it optionally.
```python
pe.bybit.stream_realtime_data(...)
# or for short:
pe.bybit.stream(...)
```


## Table of Contents
```{tableofcontents}
```