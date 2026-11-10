# `pfeed`: The Single Source of Truth for Algo-Trading Data. Uniting Traders to Clean Once & Share with All.

[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/vqpS94tpdp)
[![X](https://img.shields.io/twitter/follow/pfund_ai?style=social)](https://x.com/pfund_ai)
[![Downloads](https://static.pepy.tech/badge/pfeed/month)](https://pepy.tech/project/pfeed)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg?cacheSeconds=300)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed?cacheSeconds=300)
[![afterpython](https://afterpython.org/shield.svg)](https://afterpython.org)
[![View Code Wiki](https://assets.codewiki.google/readme-badge/static.svg)](https://codewiki.google/github.com/pfund-software-ltd/pfeed?utm_source=badge&utm_medium=github&utm_campaign=github.com/pfund-software-ltd/pfeed)
<!--[![Discussions](https://img.shields.io/badge/Discussions-Let's%20Chat-green)](https://github.com/PFund-Software-Ltd/pfeed/discussions)-->
<!-- [![PyPI version](https://badge.fury.io/py/pfeed.svg)](https://badge.fury.io/py/pfeed) -->
<!-- ![PyPI downloads](https://img.shields.io/pypi/dm/pfeed?label=downloads&cacheSeconds=86400) -->
<!-- ![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social) -->


## TL;DR: use pfeed to manage your trading data; other traders will help you clean it

<!--<img alt="PFeed Architecture" src="afterpython/static/pfeed.png">-->

## Problem
Starting algo-trading requires reliable and clean data, but traders often **work in silos**, each writing **duplicated code** to clean the same datasets—wasting time and effort. Accessing clean, ready-to-use data is a challenge, forcing traders to handle tedious data tasks before they can even start trading.

## Solution
`pfeed` leverages modern data engineering tools to centralize data cleaning efforts, automate ETL/ELT, store data in a **data lake with DeltaLake** support, and output **backtesting-ready data**, allowing traders to focus on strategy development.

---
`pfeed` (/piː fiːd/) is the data engine for trading, serving as a pipeline between raw data sources and traders. It enables you to **download historical data**, **stream real-time data**, and **store cleaned data** in a **local data lake for quantitative analysis**, supporting both **batch processing** and **streaming** workflows through streamlined data collection, cleaning, transformation, and storage.

## Core Features
- [x] Download or stream reliable, validated and **clean data** for research, backtesting, or live trading
- [x] Get historical data (**dataframe**) or live data in standardized formats by just calling one **single** function
- [x] **Own your data** — store locally now, write directly to the cloud later
- [x] Build a local data lakehouse (Parquet + [DuckLake]) on your own machine
- [x] Interact with different kinds of data (including TradFi, CeFi and DeFi) using a **unified interface**

---

<details>
<summary>Table of Contents</summary>

- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Download Historical Data](#1-download-historical-data)
    - [Retrieve Downloaded Data](#2-retrieve-downloaded-data)
    - [Stream Live Data](#3-stream-live-data)
- [Supported Data Sources](#supported-data-sources)
- [Supported IOs](#supported-ios)
- [Related Projects](#related-projects)
- [Disclaimer](#disclaimer)

</details>



## Installation
> For more installation options, please refer to the [documentation](https://pfeed.pfund.ai/doc/installation).
```bash
# pick your data source, e.g. bybit, and add ray for parallel processing (recommended).
pip install "pfeed[bybit,ray]"
```


## Quick Start
> Want to try first? Run `pfeed` in your browser — no install needed — at
 **[pfeed.pfund.ai](https://pfeed.pfund.ai/)**.
```python
import pfeed as pe

bybit = pe.Bybit()
feed = bybit.market_feed
```

### 1. Download Historical Data
Download historical data
```python
result = feed.download(
    product="BTC_USDT_PERP",  # or BTC_USDT_PERPETUAL in full
    resolution="1minute",  # '1tick'/'1t' or '2second'/'2s' etc.
    start_date="2026-01-01",
    end_date="2026-01-01",
    io=pe.ParquetIO(),  # None to skip storing
)
df = result.data.collect()
```


### 2. Retrieve Stored Data
Retrieve stored data
```python
result = feed.retrieve(
    product="BTC_USDT_PERP",
    resolution="1m",  # 1-minute data
    start_date="2026-01-01",
    end_date="2026-01-01",
    io=pe.ParquetIO(),
)
df = result.data.collect()
```


### 3. Stream Live Data
Get streaming data
```python
feed.stream(
    product="BTC_USDT_CRYPTO",
    resolution="1tick",  # or just 'tick' for tick data
    callback=lambda msg: print(msg),
)
```

[Hyperliquid]: https://hyperliquid.xyz
[Bybit]: https://bybit.com
[Binance]: https://binance.com
[OKX]: https://okx.com

## Supported Data Sources
| Data Source          | Data Categories | Download Historical Data | Stream Live Data |
| -------------------- | --------------- | ------------------------ | ---------------- |
| [Hyperliquid]        | Market Data     | 🔴                       | 🔴               |
| [Bybit]              | Market Data     | 🟢                       | 🟢               |
| [OKX]                | Market Data     | 🔴                       | 🔴               |
| [Binance]            | Market Data     | 🔴                       | 🔴               |


[Lance]: https://lance.org/
[DuckLake]: https://ducklake.select/

## Supported IOs
| IO Format   | Status |
| ----------- | ------ |
| Parquet     | 🟢     |
| [DuckLake]  | 🟢     |
| [Lance]     | 🔴     |

🟢 = finished \
🟡 = in progress \
🔴 = todo \
⚪ = not applicable \
\* = paid data


[alphafund]: https://github.com/PFund-Software-Ltd/alphafund
[pfund]: https://github.com/PFund-Software-Ltd/pfund
[pytrade]: https://pytrade.org

## Related Projects
- [alphafund] - AI-Powered Personal Hedge Fund. Be Your Own Fund Manager.
- [pfund] — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading
- [pytrade] - An Educational Hub for Trading with Python. Where Python Trading Begins


## Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This framework is intended for educational and research purposes only. It should not be used for real trading without understanding the risks involved. Trading in financial markets involves significant risk, and there is always the potential for loss. Your trading results may vary. No representation is being made that any account will or is likely to achieve profits or losses similar to those discussed on this platform.

The developers of this framework are not responsible for any financial losses incurred from using this software. This includes but not limited to losses resulting from inaccuracies in any financial data output by `pfeed`. Users should conduct their due diligence, verify the accuracy of any data produced by `pfeed`, and consult with a professional financial advisor before engaging in real trading activities.
