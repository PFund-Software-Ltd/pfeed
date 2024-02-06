# PFeed: Data Pipeline for Algo-Trading, Getting and Storing Real-Time and Historical Data Made Easy.

[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)

PFeed (/piː fiːd/) is a data integration library tailored for algorithmic trading, 
serving as an ETL (Extract, Transform, Load) data pipeline between raw data sources and traders,
helping them in creating a local data lake for quantitative research.

PFeed allows traders to download historical, paper, and live data from various data sources, both free and paid,
and stores them into a local data lake using [MinIO](https://min.io/).

It is designed to be used alongside [PFund](https://github.com/PFund-Software-Ltd/pfund) — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading.

<details>
<summary>Table of Contents</summary>

- [Project Status](#project-status)
- [Mission](#mission)
- [Core Features](#core-features)
- [Installation](#installation)
- [Quick Start](#quick-start)
    - [Download Historical Data on Command Line](#download-historical-data-on-command-line)
    - [Download Historical Data in Python](#download-historical-data-in-python)
- [Supported Data Sources](#supported-data-sources)
- [Related Projects](#related-projects)

</details>


## Project Status
**_Caution: PFeed is at a VERY EARLY stage, use it at your own risk._**

PFeed is currently under active development, the framework design will be prioritized first over
stability and scalability. 

Please note that the available version is a *dev* version, not a *stable* one. \
You are encouraged to play with the *dev* version, but only use it when a *stable* version is released.

> PFeed currently only supports [Bybit](https://bybit.com/) and Yahoo Finance for testing purpose.

## Mission
Algo-trading has always been a complicated task due to the multitude of components and procedures involved. \
Data collection and processing is probably the most mundane and yet critical part of it, as all results and findings 
are derived from the data.

However, preparing this data for use is not quick and easy. For example, sometimes even when the data is available (e.g. [Bybit data](https://public.bybit.com/trading/)), it is often in raw form and requires some cleaning.

> PFeed's mission is to **_free traders from the tedious data work_** by providing cleaned data in a standard format that is ready for use, making them significantly faster to get to the analysis and strategy development phase.


## Core Features
- Unified approach to interact with different data sources 
- ETL data pipline to transform raw data and store them in [MinIO](https://min.io/)
- Using [Ray](https://github.com/ray-project/ray) to download data in parallel


## Installation
```bash
poetry add pfeed
```


## Quick Start
### Download Historical Data on Command Line
```bash
pfeed download -s bybit -p BTC_USDT_PERP --no-minio
```

### Download Historical Data in Python
```python
from pfeed import bybit
bybit.download(pdts=['BTC_USDT_PERP'])
```


## Supported Data Sources
| Data Source                                  | Get Historical Data | Download Historical Data | Get Live/Paper Data | Download Live/Paper Data |
| -------------------------------------------- | ------------------- | ------------------------ | ------------------- | ------------------------ |
| Yahoo Finance                                | 🟢                  | ⚪                       | ⚪                  | ⚪                       |
| Bybit                                        | 🟢                  | 🟢                       | 🟡                  | 🔴                       |
| *Interactive Brokers (IB)                    | 🔴                  | ⚪                       | 🔴                  | 🔴                       |
| [*FirstRate Data](https://firstratedata.com) | 🔴                  | 🔴                       | ⚪                  | ⚪                       |
| Binance                                      | 🔴                  | 🔴                       | 🔴                  | 🔴                       |
| OKX                                          | 🔴                  | 🔴                       | 🔴                  | 🔴                       |

🟢 = finished \
🟡 = in progress \
🔴 = todo \
⚪ = not applicable \
\* = Paid/Non-Free data \
get data = store it in memory for python to use \
download data = store it in local machine for later research


## Related Projects
- [PFund](https://github.com/PFund-Software-Ltd/pfund) — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading