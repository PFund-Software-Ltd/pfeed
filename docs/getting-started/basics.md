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

[FirstRate Data]: https://firstratedata.com
[Bybit Data]: https://public.bybit.com
[Binance Data]: https://data.binance.vision
[OKX Data]: https://www.okx.com/data-download
[PFund's Basics]: https://pfund-docs.pfund.ai/getting-started/basics.html
[PFund's Conventions]: https://pfund-docs.pfund.ai/getting-started/conventions.html

# Basics
Some basic concepts and usage in pfeed will be explained here.

## Data Feeds
In pfeed, data feeds are objects that serve as high-level interfaces designed for straightforwrad interaction with the data sources. The object `feed` in the following setup code snippet is an example of a data feed object:
```{code-cell}
:tags: [hide-output]

import pfeed as pe
from pfeed.config_handler import ConfigHandler

# HACK: increase the log level to hide some log outputs in the following examples
config = ConfigHandler(
    logging_config={ 'handlers': { 'stream_handler': {'level': 'critical'} } }
)
feed = pe.BybitFeed(config=config)
```

To print out the supported data feeds in pfeed, you can use the following code snippet:
```{code-cell}
:tags: [hide-output]

from pfeed.const.commons import SUPPORTED_DATA_FEEDS
from pprint import pprint

pprint(SUPPORTED_DATA_FEEDS)
```


## Data Types (dtypes)
In pfeed, the term dtype refers to the granularity of the data, indicating the level of detail at which the data is recorded or aggregated. 
pfeed supports a variety of data types, categorized into two main groups:
- Raw data types: **'raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily'**
- Aggregated data types: **'tick', 'second', 'minute', 'hour', 'daily'** 

### Raw Data Types
Data types prefixed with `raw_` represent **raw data types**. These are the data types closest to the raw data that is directly obtained from the data sources. *Only columns renaming and values mapping (e.g. mapping 'buy' to 1 and 'sell'to -1 for calculation convenience) are applied to standardize different naming conventions* from different data sources, everything else is intact.

For example, the data granularity of [Bybit Data] is `tick` data (tick-level trade data), so the corresponding raw data type for Bybit data in pfeed is `raw_tick`. Attempting to fetch data using a raw data type that is unsupported by the data source will result in errors. Here is the example of Bybit raw data:

#### Raw Tick Data
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

### Aggregated Data Types
Aggregated Data Types are the data types that are aggregated from raw data types. The aggregation process includes data cleaning, data transformation, and data aggregation. The following are some examples:

#### Tick Data
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

#### Minute Data
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


## CLI Commands
### Download Historical Data
```bash
# download data, default data type (dtype) is 'raw' data
pfeed download -d BYBIT -p BTC_USDT_PERP --start-date 2024-03-01 --end-date 2024-03-08

# download multiple products BTC_USDT_PERP and ETH_USDT_PERP and minute data
pfeed download -d BYBIT -p BTC_USDT_PERP -p ETH_USDT_PERP --dtype minute

# download all perpetuals data from bybit
pfeed download -d BYBIT --ptype PERP

# download all the data from bybit (CAUTION: your local machine probably won't have enough space for this!)
pfeed download -d BYBIT

# store data into MinIO (need to start MinIO by running `pfeed docker-compose up -d` first)
pfeed download -d BYBIT -p BTC_USDT_PERP --use-minio

# enable debug mode and turn off using Ray
pfeed download -d BYBIT -p BTC_USDT_PERP --debug --no-ray
```

### List Current Config
```bash
# list the current config:
pfeed config --list

# change the data storage location to your local project's 'data' folder:
pfeed config --data-path ./data

# for more commands:
pfeed --help
```

### Run PFeed's docker-compose.yml
```bash
# same as 'docker-compose', only difference is it has pointed to pfeed's docker-compose.yml file
pfeed docker-compose [COMMAND]

# e.g. start services
pfeed docker-compose up -d

# e.g. stop services
pfeed docker-compose down
```


```{seealso}
For other standards and conventions used in `pfeed`, please refer to [PFund's Basics] and [PFund's Conventions]
```