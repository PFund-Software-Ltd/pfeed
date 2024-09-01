# PFeed: Data Pipeline for Algo-Trading, Getting and Storing Real-Time and Historical Data Made Easy.

![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social)
![PyPI downloads](https://img.shields.io/pypi/dm/pfeed?label=downloads)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)
[![Jupyter Book Badge](https://raw.githubusercontent.com/PFund-Software-Ltd/pfeed/main/docs/images/jupyterbook.svg)](https://jupyterbook.org)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)

[MinIO]: https://min.io/
[PFund]: https://github.com/PFund-Software-Ltd/pfund
[Ray]: https://github.com/ray-project/ray
[Polars]: https://github.com/pola-rs/polars
[Prefect]: https://www.prefect.io
[Timescaledb]: https://www.timescale.com/
[Dask]: https://www.dask.org/
[Spark]: https://spark.apache.org/docs/latest/api/python/index.html
[DuckDB]: https://github.com/duckdb/duckdb
[Daft]: https://github.com/Eventual-Inc/Daft
[PyTrade.org]: https://pytrade.org
[Databento]: https://databento.com/
[Polygon]: https://polygon.io/
[Bybit]: https://bybit.com/
[FirstRate Data]: https://firstratedata.com

## Problem
Starting algo-trading requires reliable, clean data. However, the time-consuming and mundane tasks of data cleaning and storage often discourage traders from embarking on their algo-trading journey.

## Solution
By leveraging modern data engineering tools, `pfeed` handles the tedious data work and **outputs backtesting-ready data**, accelerating traders to get to the strategy development phase.

---
PFeed (/piː fiːd/) is a data pipeline for algorithmic trading, serving as a bridge between raw data sources and traders by automating the process of data collection, cleaning, transformation, and storage, loading clean data into a **local data lake for quantitative analysis**.

## Core Features
- [x] Unified approach for interacting with various data sources and obtaining historical and live data
- [x] ETL data pipline for transforming raw data to clean data and storing it in [MinIO] (optional)
- [x] Fast data downloading, utilizing [Ray] for parallelization
- [x] Supports multiple data tools (e.g. Pandas, [Polars], [Dask], [Spark], [DuckDB], [Daft])
- [ ] Integrates with [Prefect] to control data flows
- [ ] Listens to PFund's trade engine and adds trade history to a local database [Timescaledb] (optional)

> It is designed to be used alongside [PFund] — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready.

---

<details>
<summary>Table of Contents</summary>

- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Main Usage: Data Feed](#main-usage-data-feed)
    - [Download Historical Data on Command Line](#download-historical-data-on-command-line)
    - [Download Historical Data in Python](#download-historical-data-in-python)
    - [List Current Config](#list-current-config)
    - [Run PFeed's docker-compose.yml](#run-pfeeds-docker-composeyml)
- [Supported Data Sources](#supported-data-sources)
- [Supported Data Tools](#supported-data-tools)
- [Related Projects](#related-projects)
- [Disclaimer](#disclaimer)

</details>


## Installation
### Using [Poetry](https://python-poetry.org) (Recommended)
```bash
# [RECOMMENDED]: Download data (e.g. Bybit and Yahoo Finance) + Data tools (e.g. pandas, polars) + Data storage (e.g. MinIO) + Boosted performance (e.g. Ray)
poetry add "pfeed[all]"

# [Download data + Data tools + Data storage]
poetry add "pfeed[df,data]"

# [Download data + Data tools]
poetry add "pfeed[df]"

# [Download data only]:
poetry add pfeed

# update to the latest version:
poetry update pfeed
```

### Using Pip
```bash
# same as above, you can choose to install "pfeed[all]", "pfeed[df,data]", "pfeed[df]" or "pfeed"
pip install "pfeed[all]"

# install the latest version:
pip install -U pfeed
```

### Checking your installation
```bash
$ pfeed --version
```

## Quick Start
### 1. Get Historical Data in Dataframe (No storage)
Get [Bybit]'s data in dataframe, e.g. 1-minute data (data is downloaded on the fly if not stored locally)

```python
import pfeed as pe

feed = pe.BybitFeed(data_tool='polars')

df = feed.get_historical_data(
    'BTC_USDT_PERP',
    resolution='1minute',  # 'raw' or '1tick'/'1t' or '2second'/'2s' etc.
    start_date='2024-03-01',
    end_date='2024-03-01',
)
```

Printing the first few rows of `df`:
|    | ts                  | product       | resolution   |    open |    high |     low |   close |   volume |
|---:|:--------------------|:--------------|:-------------|--------:|--------:|--------:|--------:|---------:|
|  0 | 2024-03-01 00:00:00 | BTC_USDT_PERP | 1m           | 61184.1 | 61244.5 | 61175.8 | 61244.5 |  159.142 |
|  1 | 2024-03-01 00:01:00 | BTC_USDT_PERP | 1m           | 61245.3 | 61276.5 | 61200.7 | 61232.2 |  227.242 |
|  2 | 2024-03-01 00:02:00 | BTC_USDT_PERP | 1m           | 61232.2 | 61249   | 61180   | 61184.2 |   91.446 |

> By using pfeed, you are just a few lines of code away from a standardized dataframe, how convenient!



### 2. Download Historical Data on the Command Line Interface (CLI)
```bash
# download data, default data type (dtype) is 'raw' data
pfeed download -d BYBIT -p BTC_USDT_PERP --start-date 2024-03-01 --end-date 2024-03-08

# download multiple products BTC_USDT_PERP and ETH_USDT_PERP and minute data
pfeed download -d BYBIT -p BTC_USDT_PERP -p ETH_USDT_PERP --dtypes minute

# download all perpetuals data from bybit
pfeed download -d BYBIT --ptypes PERP

# download all the data from bybit (CAUTION: your local machine probably won't have enough space for this!)
pfeed download -d BYBIT

# store data into MinIO (need to start MinIO by running `pfeed docker-compose up -d` first)
pfeed download -d BYBIT -p BTC_USDT_PERP --use-minio

# enable debug mode and turn off using Ray
pfeed download -d BYBIT -p BTC_USDT_PERP --debug --no-ray
```

### 3. Download Historical Data in Python
```python
import pfeed as pe

# compared to the CLI approach, this approach is more convenient for downloading multiple products
pe.download(
    data_source='bybit',
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
| Yahoo Finance             | 🟢                  | ⚪                        | ⚪                  | ⚪                     |
| Bybit                     | 🟢                  | 🟢                        | 🟡                  | 🔴                     |
| *Interactive Brokers (IB) | 🔴                  | ⚪                        | 🔴                  | 🔴                     |
| *[FirstRate Data]         | 🔴                  | 🔴                        | ⚪                  | ⚪                     |
| [Databento]               | 🔴                  | 🔴                        | 🔴                  | 🔴                     |
| [Polygon]                 | 🔴                  | 🔴                        | 🔴                  | 🔴                     |
| Binance                   | 🔴                  | 🔴                        | 🔴                  | 🔴                     |
| OKX                       | 🔴                  | 🔴                        | 🔴                  | 🔴                     |

🟢 = finished \
🟡 = in progress \
🔴 = todo \
⚪ = not applicable \
\* = paid data


## Supported Data Tools
| Data Tools               | Supported |
| ------------------------ | --------- |
| Pandas                   | 🟢        |
| [Polars]                 | 🟢        |
| [Dask]                   | 🔴        |
| [Spark]                  | 🔴        |
| [DuckDB]                 | 🔴        |
| [Daft]                   | 🔴        |


## Related Projects
- [PFund] — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading
- [PyTrade.org] - A curated list of Python libraries and resources for algorithmic trading.


## Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This framework is intended for educational and research purposes only. It should not be used for real trading without understanding the risks involved. Trading in financial markets involves significant risk, and there is always the potential for loss. Your trading results may vary. No representation is being made that any account will or is likely to achieve profits or losses similar to those discussed on this platform.

The developers of this framework are not responsible for any financial losses incurred from using this software. This includes but not limited to losses resulting from inaccuracies in any financial data output by PFeed. Users should conduct their due diligence, verify the accuracy of any data produced by PFeed, and consult with a professional financial advisor before engaging in real trading activities.
