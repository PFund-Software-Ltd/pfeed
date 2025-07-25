# PFeed: The Single Source of Truth for Algo-Trading Data. Uniting Traders to Clean Once & Share with All.

[![Twitter Follow](https://img.shields.io/twitter/follow/pfund_ai?style=social)](https://x.com/pfund_ai)
![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social)
![PyPI downloads](https://img.shields.io/pypi/dm/pfeed?label=downloads)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)
![Discussions](https://img.shields.io/badge/Discussions-Let's%20Chat-green)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/PFund-Software-Ltd/pfeed)
<!-- [![Jupyter Book Badge](https://raw.githubusercontent.com/PFund-Software-Ltd/pfeed/main/docs/images/jupyterbook.svg)](https://jupyterbook.org) -->
<!-- [![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/) -->

[MinIO]: https://min.io/
[Deltalake]: https://github.com/delta-io/delta-rs
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
[Prefect]: https://www.prefect.io/

> **This library is NOT ready for use, please wait for 0.1.0 release.**

## TL;DR: use pfeed to manage your trading data; other traders will help you clean it
![PFeed Architecture](./docs/images/pfeed.png)
> For illustration purposes only, not everything shown is currently supported.

## Problem
Starting algo-trading requires reliable and clean data, but traders often **work in silos**, each writing **duplicated code** to clean the same datasets—wasting time and effort. Accessing clean, ready-to-use data is a challenge, forcing traders to handle tedious data tasks before they can even start trading.

## Solution
`pfeed` leverages modern data engineering tools to centralize data cleaning efforts, automate ETL/ELT, store data in a **data lake with Delta Lake** support, and output **backtesting-ready data**, allowing traders to focus on strategy development.

---
PFeed (/piː fiːd/) is the data engine for trading, serving as a pipeline between raw data sources and traders. It enables you to **download historical data**, **stream real-time data**, and **store cleaned data** in a **local data lake for quantitative analysis**, supporting both **batch processing** and **streaming** workflows through streamlined data collection, cleaning, transformation, and storage.

## Core Features
- [x] Download or stream reliable, validated and **clean data** for research, backtesting, or live trading
- [x] Get historical data (**dataframe**) or live data in standardized formats by just calling a **single** function
- [x] **Own your data** by storing them locally using [MinIO] + [Deltalake], or in the cloud
- [x] Interact with different kinds of data (including TradFi, CeFi and DeFi) using a **unified interface**
- [x] Scale using modern data tools (e.g. [Polars], [Dask]) and workflow orchestration frameworks (e.g. [Prefect] for batch processing)

---

<details>
<summary>Table of Contents</summary>

- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Get Historical Data in Dataframe](#1-get-historical-data-in-dataframe)
    - [Download Historical Data](#2-download-historical-data)
- [Supported Data Sources](#supported-data-sources)
- [Related Projects](#related-projects)
- [Disclaimer](#disclaimer)

</details>



## Installation
> For more installation options, please refer to the [documentation](https://pfeed-docs.pfund.ai/installation).
```bash
# [RECOMMENDED]: Core Features, including MinIO, DeltaLake, Ray, Prefect, etc.
pip install -U "pfeed[core]"

# add your desired data sources, e.g. databento, polygon, etc.
pip install -U "pfeed[core,databento,polygon]"

# Minimal Features
pip install -U "pfeed"
```



## Quick Start
Create data feed object
```python
import pfeed as pe

bybit = pe.Bybit(data_tool='polars')
feed = bybit.market_feed  # this could be xxx.news_feed if the data source supports it
```

### 1. Get Historical Data in Dataframe
Get [Bybit]'s data in dataframe, e.g. 1-minute data (data is downloaded on the fly if not found in storage)

```python
df = feed.get_historical_data(
    product='BTC_USDT_PERP',  # or BTC_USDT_PERPETUAL in full
    resolution='1minute',  # '1tick'/'1t' or '2second'/'2s' etc.
    start_date='2025-01-01',
    end_date='2025-01-01',
)
```

<details>
<summary>See the first few rows of df:</summary>

| date                | resolution   | product       | symbol   |    open |    high |     low |   close |   volume |
|:--------------------|:-------------|:--------------|:---------|--------:|--------:|--------:|--------:|---------:|
| 2025-01-01 00:00:00 | 1m           | BTC_USDT_PERP | BTCUSDT  | 93530   | 93590.8 | 93501.3 | 93590.5 |   30.284 |
| 2025-01-01 00:01:00 | 1m           | BTC_USDT_PERP | BTCUSDT  | 93590.5 | 93627.7 | 93571.8 | 93625   |   30.334 |
</details>

> By using pfeed, you are just **one function call** away from getting a standardized dataframe


### 2. Download Historical Data
Download historical data to the storage (e.g. local, MinIO, DuckDB etc.)
```python
feed.download(
    product='ETH_USDT_SPOT',
    resolution='1s',  # 1-second data
    rollback_period='1w',  # rollback 1 week
    to_storage='local',
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
