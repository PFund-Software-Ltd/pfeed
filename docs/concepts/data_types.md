---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.5
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Data Types
In pfeed, the term `dtype` (data type) refers to the granularity of the data, indicating the level of detail at which the data is recorded or aggregated. 
pfeed supports a variety of data types, categorized into two main groups:
- Raw data types: **'raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily'**
- Aggregated data types: **'tick', 'second', 'minute', 'hour', 'daily'** 

## Raw Data Types
Data types prefixed with `raw_` represent **raw data types**. These are the data types closest to the raw data that is directly obtained from the data sources. *Only columns renaming and values mapping (e.g. mapping 'buy' to 1 and 'sell'to -1 for calculation convenience) are applied to standardize different naming conventions* from different data sources, everything else is intact.

For example, the data granularity of [Bybit Data] is `tick` data (tick-level trade data), so the corresponding raw data type for Bybit data in pfeed is `raw_tick`. Attempting to fetch data using a raw data type that is unsupported by the data source will result in errors. Here is the example of Bybit raw data:

### Raw Tick Data
```{code-cell}
df = feed.get_historical_data(
    'BTC_USDT_PERP',
    resolution='raw_tick',  # or use 'raw' implicitly
    start_date='2024-03-01',
    end_date='2024-03-01',
    data_tool='polars',  # or 'pandas'
)
print(df.head())
```

> For convenience, you can use **`raw` as an implicit raw data type**. In the context of [Bybit Data], `raw` will be implicitly converted to `raw_tick` in pfeed. If there are multiple raw data types available in the data source, the most granular one will be used.

## Aggregated Data Types
Aggregated Data Types are the data types that are aggregated from raw data types. The aggregation process includes data cleaning, data transformation, and data aggregation. The following are some examples:

### Tick Data
```{code-cell}
df = feed.get_historical_data(
    'BTC_USDT_PERP',
    # resolution = period + timeframe, e.g. 1t (1tick), 2s (2second), 3m (3minute) etc.
    resolution='1tick',
    start_date='2024-03-01',
    end_date='2024-03-01',
    data_tool='polars',  # or 'pandas'
)
print(df.head())
```

### Minute Data
```{code-cell}
df = feed.get_historical_data(
    'BTC_USDT_PERP',
    resolution='2m',  # 2-minute data
    start_date='2024-03-01',
    end_date='2024-03-01',
    data_tool='polars',  # or 'pandas'
)
print(df.head())
```

```{note}
Tick data generally means data at tick level, it could be trade data or orderbook data. However, **in pfeed, tick data refers to trade data** because orderbook data is less common in retail trading. If pfeed supports orderbook data in the future, it will probably be named 'orderbook_l1' for orderbook level1 data and 'orderbook_l2' for orderbook level2 data etc.
```