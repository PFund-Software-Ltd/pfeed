# PFeed: Data Pipeline for Algo-Trading, Getting and Storing Real-Time and Historical Data Made Easy.

[![Twitter Follow](https://img.shields.io/twitter/follow/pfund_ai?style=social)](https://x.com/pfund_ai)
![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social)
![PyPI downloads](https://img.shields.io/pypi/dm/pfeed?label=downloads)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)
<!-- [![Jupyter Book Badge](https://raw.githubusercontent.com/PFund-Software-Ltd/pfeed/main/docs/images/jupyterbook.svg)](https://jupyterbook.org) -->
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)

[MinIO]: https://min.io/
[PFund]: https://github.com/PFund-Software-Ltd/pfund
[Polars]: https://github.com/pola-rs/polars
[Dask]: https://www.dask.org/
[Spark]: https://spark.apache.org/docs/latest/api/python/index.html
[PyTrade.org]: https://pytrade.org
[Yahoo Finance]: https://github.com/ranaroussi/yfinance
[Bybit]: https://public.bybit.com
[Binance]: https://data.binance.vision
[OKX]: https://www.okx.com/data-download
[Databento]: https://databento.com/
[Polygon]: https://polygon.io/
[FirstRate Data]: https://firstratedata.com

## Problem
Starting algo-trading requires reliable, clean data. However, the time-consuming and mundane tasks of data cleaning and storage often discourage traders from embarking on their algo-trading journey.

## Solution
By leveraging modern data engineering tools, `pfeed` handles the tedious data work and **outputs backtesting-ready data**, allowing traders to focus on strategy development.

---
PFeed (/piː fiːd/) is a data pipeline for algorithmic trading, serving as a bridge between raw data sources and traders. It enables you to **download historical data**, **stream real-time data**, and **store cleaned data** in a **local data lake for quantitative analysis**, by automating the processes of data collection, cleaning, transformation, and storage.

## Core Features
- [x] Download or stream reliable, validated and **clean data** for research, backtesting, or live trading
- [x] Get historical data (**dataframe**) or live data in standardized formats by just calling a **single** function
- [x] **Own your data** by storing them locally using [MinIO], with the option to connect to the cloud
- [x] Interact with different kinds of data (including TradFi, CeFi and DeFi) using a **unified interface**

---

<details>
<summary>Table of Contents</summary>

- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Get Historical Data in Dataframe](#1-get-historical-data-in-dataframe-no-storage)
    - [Download Historical Data on Command Line](#2-download-historical-data-on-the-command-line-interface-cli)
    - [Download Historical Data in Python](#3-download-historical-data-in-python)
- [Supported Data Sources](#supported-data-sources)
- [Related Projects](#related-projects)
- [Disclaimer](#disclaimer)

</details>



## Installation
> For more installation options, please refer to the [documentation](https://pfeed-docs.pfund.ai/installation).
```bash
# [RECOMMENDED]: Full Features, choose this if you do not care about the package size
pip install -U "pfeed[all]"

# Minimal Features, only supports getting, downloading and streaming data
pip install -U "pfeed[core]"
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

> By using pfeed, you are just a few lines of code away from getting a standardized dataframe, how convenient!

### 2. Download Historical Data on the Command Line Interface (CLI)
> For more CLI commands, please refer to the [documentation](https://pfeed-docs.pfund.ai/cli-commands).
```bash
# download data, default data type (dtype) is 'raw' data
pfeed download -d BYBIT -p BTC_USDT_PERP --start-date 2024-03-01 --end-date 2024-03-08

# download multiple products BTC_USDT_PERP and ETH_USDT_PERP as minute data and store them locally
pfeed download -d BYBIT -p BTC_USDT_PERP -p ETH_USDT_PERP --dtypes minute --use-minio
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



## Supported Data Sources
| Data Source          | Get Historical Data | Download Historical Data | Get Live Data | Stream Live Data |
| -------------------- | ------------------- | ------------------------ | --------------| ---------------- |
| [Yahoo Finance]      | 🟢                  | ⚪                        | ⚪            | ⚪               |
| [Bybit]              | 🟢                  | 🟢                        | 🟡            | 🔴               |
| *Interactive Brokers | 🔴                  | ⚪                        | 🔴            | 🔴               |
| *[FirstRate Data]    | 🔴                  | 🔴                        | ⚪            | ⚪               |
| *[Databento]         | 🔴                  | 🔴                        | 🔴            | 🔴               |
| *[Polygon]           | 🔴                  | 🔴                        | 🔴            | 🔴               |
| [Binance]            | 🔴                  | 🔴                        | 🔴            | 🔴               |
| [OKX]                | 🔴                  | 🔴                        | 🔴            | 🔴               |

🟢 = finished \
🟡 = in progress \
🔴 = todo \
⚪ = not applicable \
\* = paid data



## Related Projects
- [PFund] — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading
- [PyTrade.org] - A curated list of Python libraries and resources for algorithmic trading.



## Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This framework is intended for educational and research purposes only. It should not be used for real trading without understanding the risks involved. Trading in financial markets involves significant risk, and there is always the potential for loss. Your trading results may vary. No representation is being made that any account will or is likely to achieve profits or losses similar to those discussed on this platform.

The developers of this framework are not responsible for any financial losses incurred from using this software. This includes but not limited to losses resulting from inaccuracies in any financial data output by PFeed. Users should conduct their due diligence, verify the accuracy of any data produced by PFeed, and consult with a professional financial advisor before engaging in real trading activities.
