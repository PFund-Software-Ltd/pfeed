# PFeed: Data Pipeline for Algo-Trading, Getting and Storing Real-Time and Historical Data Made Easy.

![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social)
![PyPI downloads](https://img.shields.io/pypi/dm/pfeed?label=downloads)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)
[![Jupyter Book Badge](https://raw.githubusercontent.com/PFund-Software-Ltd/pfeed/main/docs/images/jupyterbook.svg)](https://jupyterbook.org)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)

PFeed (/piː fiːd/) is a data integration library tailored for algorithmic trading, 
serving as an ETL (Extract, Transform, Load) data pipeline between raw data sources and traders,
helping them in creating a **local data lake for quantitative research**.

PFeed allows traders to download historical, paper, and live data from various data sources, both free and paid,
and stores them into a local data lake using [MinIO](https://min.io/).

It is designed to be used alongside [PFund](https://github.com/PFund-Software-Ltd/pfund) — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading, or as a standalone package.

<details>
<summary>Table of Contents</summary>

- [Project Status](#project-status)
- [Mission](#mission)
- [Core Features](#core-features)
- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Main Usage: Data Feed](#main-usage-data-feed)
    - [Download Historical Data on Command Line](#download-historical-data-on-command-line)
    - [Download Historical Data in Python](#download-historical-data-in-python)
    - [List Current Config](#list-current-config)
    - [Run PFeed's docker-compose.yml](#run-pfeeds-docker-composeyml)
- [Supported Data Sources](#supported-data-sources)
- [Related Projects](#related-projects)

</details>


## Project Status
**_Caution: PFeed is at a VERY EARLY stage, use it at your own risk._**

PFeed is currently under active development, the framework design will be prioritized first over
stability and scalability. 

Please note that the available version is a *dev* version, not a *stable* one. \
You are encouraged to play with the *dev* version, but only use it when a *stable* version is released.

> PFeed for the time being only supports [Bybit](https://bybit.com/) and Yahoo Finance for testing purpose.

## Mission
Algo-trading has always been a complicated task due to the multitude of components and procedures involved. \
Data collection and processing is probably the most mundane and yet critical part of it, as all results and findings 
are derived from the data.

However, preparing this data for use is not quick and easy. For example, sometimes even when the data is available (e.g. [Bybit data](https://public.bybit.com/trading/)), it is often in raw form and requires some cleaning.

> PFeed's mission is to **_free traders from the tedious data work_** by providing cleaned data in a standard format that is ready for use, making them significantly faster to get to the analysis and strategy development phase.


## Core Features
- [x] Unified approach for interacting with various data sources, obtaining historical and real-time data
- [x] ETL data pipline for transforming raw data and storing it in [MinIO](https://min.io/) (optional)
- [x] Utilizes [Ray](https://github.com/ray-project/ray) for parallel data downloading
- [x] Supports Pandas, [Polars](https://github.com/pola-rs/polars) as data tools
- [ ] Integrates with [Prefect](https://www.prefect.io) to control data flows
- [ ] Listens to PFund's trade engine and adds trade history to a local database [Timescaledb](https://www.timescale.com/) (optional)


## Installation
### Using [Poetry](https://python-poetry.org) (Recommended)
```bash
poetry add pfeed
```

### Using Pip
```bash
pip install pfeed
```


## Quick Start
### Main Usage: Data Feed
1. Download bybit raw data on the fly if not stored locally

    ```python
    import pfeed as pe

    feed = pe.BybitFeed()

    # df is a dataframe or a lazyframe (lazily loaded dataframe)
    df = feed.get_historical_data(
        'BTC_USDT_PERP',
        resolution='raw',
        start_date='2024-03-01',
        end_date='2024-03-01',
        data_tool='polars',  # or 'pandas'
    )
    ```

    > By using pfeed, you are just one line of code away from playing with e.g. bybit data, how convenient!

    Printing the first few rows of `df`:
    |    | ts                            | symbol   |   side |   volume |   price | tickDirection   | trdMatchID                           |   grossValue |   homeNotional |   foreignNotional |
    |---:|:------------------------------|:---------|-------:|---------:|--------:|:----------------|:-------------------------------------|-------------:|---------------:|------------------:|
    |  0 | 2024-03-01 00:00:00.097599983 | BTCUSDT  |      1 |    0.003 | 61184.1 | ZeroMinusTick   | 79ac9a21-0249-5985-b042-906ec7604794 |  1.83552e+10 |          0.003 |           183.552 |
    |  1 | 2024-03-01 00:00:00.098299980 | BTCUSDT  |      1 |    0.078 | 61184.9 | PlusTick        | 2af4e516-8ff4-5955-bb9c-38aa385b7b44 |  4.77242e+11 |          0.078 |          4772.42  |

2. Get dataframe with different resolution, e.g. 1-minute data
    ```python
    import pfeed as pe

    feed = pe.BybitFeed()

    # df is a dataframe or a lazyframe (lazily loaded dataframe)
    df = feed.get_historical_data(
        'BTC_USDT_PERP',
        resolution='1minute',  # or '1tick'/'1t', '2second'/'2s', '3minute'/'3m' etc.
        start_date='2024-03-01',
        end_date='2024-03-01',
        data_tool='polars',
    )
    ```
    > If you will be interacting with the data frequently, you should consider downloading it to your local machine.

    Printing the first few rows of `df`:
    |    | ts                  | product       | resolution   |    open |    high |     low |   close |   volume |
    |---:|:--------------------|:--------------|:-------------|--------:|--------:|--------:|--------:|---------:|
    |  0 | 2024-03-01 00:00:00 | BTC_USDT_PERP | 1m           | 61184.1 | 61244.5 | 61175.8 | 61244.5 |  159.142 |
    |  1 | 2024-03-01 00:01:00 | BTC_USDT_PERP | 1m           | 61245.3 | 61276.5 | 61200.7 | 61232.2 |  227.242 |
    |  2 | 2024-03-01 00:02:00 | BTC_USDT_PERP | 1m           | 61232.2 | 61249   | 61180   | 61184.2 |   91.446 |


3. pfeed also supports simple wrapping of [yfinance](https://github.com/ranaroussi/yfinance)
    ```python
    import pfeed as pe

    feed = pe.YahooFinanceFeed()

    # you can still use any kwargs supported by yfinance's ticker.history(...)
    # e.g. 'prepost', 'auto_adjust' etc.
    yfinance_kwargs = {}

    df = feed.get_historical_data(
        'AAPL',
        resolution='1d',
        start_date='2024-03-01',
        end_date='2024-03-20',
        **yfinance_kwargs
    )
    ```
    > Note that YahooFinanceFeed doesn't support the kwarg `data_tool`, e.g. polars
    
    Printing the first few rows of `df`:
    | ts                  | symbol   | resolution   |   open |   high |    low |   close |   volume |   dividends |   stock_splits |
    |:--------------------|:---------|:-------------|-------:|-------:|-------:|--------:|---------:|------------:|---------------:|
    | 2024-03-01 05:00:00 | AAPL     | 1d           | 179.55 | 180.53 | 177.38 |  179.66 | 73488000 |           0 |              0 |
    | 2024-03-04 05:00:00 | AAPL     | 1d           | 176.15 | 176.9  | 173.79 |  175.1  | 81510100 |           0 |              0 |
    | 2024-03-05 05:00:00 | AAPL     | 1d           | 170.76 | 172.04 | 169.62 |  170.12 | 95132400 |           0 |              0 |



### Download Historical Data on the Command Line Interface (CLI)
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

### Download Historical Data in Python
```python
import pfeed as pe

# compared to the CLI approach, this is more convenient for downloading multiple products
pe.bybit.download(
    pdts=[
        'BTC_USDT_PERP',
        'ETH_USDT_PERP',
        'BCH_USDT_PERP',
    ],
    dtypes=['raw'],  # data types, e.g. 'raw', 'tick', 'second', 'minute' etc.
    start_date='2024-03-01',
    end_date='2024-03-08',
    use_minio=False,
)
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


## Supported Data Sources
| Data Source               | Get Historical Data | Download Historical Data | Get Live/Paper Data | Stream Live/Paper Data |
| ------------------------- | ------------------- | ------------------------ | ------------------- | ---------------------- |
| Yahoo Finance             | 🟢                  | ⚪                       | ⚪                  | ⚪                     |
| Bybit                     | 🟢                  | 🟢                       | 🟡                  | 🔴                     |
| *Interactive Brokers (IB) | 🔴                  | ⚪                       | 🔴                  | 🔴                     |
| *[FirstRate Data]         | 🔴                  | 🔴                       | ⚪                  | ⚪                     |
| Binance                   | 🔴                  | 🔴                       | 🔴                  | 🔴                     |
| OKX                       | 🔴                  | 🔴                       | 🔴                  | 🔴                     |

[FirstRate Data]: https://firstratedata.com

🟢 = finished \
🟡 = in progress \
🔴 = todo \
⚪ = not applicable \
\* = paid data


## Related Projects
- [PFund](https://github.com/PFund-Software-Ltd/pfund) — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading
- [PyTrade.org](https://pytrade.org) - A curated list of Python libraries and resources for algorithmic trading.