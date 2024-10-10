[yfinance]: https://github.com/ranaroussi/yfinance
[QuantConnect]: https://www.quantconnect.com
[FirstRate Data]: https://firstratedata.com
[Databento]: https://databento.com
[Polygon]: https://polygon.io
[Bybit Data]: https://public.bybit.com
[Binance Data]: https://data.binance.vision
[OKX Data]: https://www.okx.com/data-download
[MinIO]: https://min.io/
[Bybit]: https://bybit.com

# Overview

```{attention}
This project is **under active development** and the API is subject to change. Some features are not yet implemented but only documented for future reference.

Current Features:
- Get, download, and store cleaned historical data from Yahoo Finance and [Bybit]

The current focus is to establish a well-designed, extensible foundation for the framework. Once it is stable, efforts will shift toward adding more data sources.
```

---

## What is `pfeed`
PFeed (/piː fiːd/) is a data pipeline for algorithmic trading, serving as a bridge between raw data sources and traders. It enables you to **download historical data**, **stream real-time data**, and **store cleaned data** in a **local data lake for quantitative analysis**, by automating the processes of data collection, cleaning, transformation, and storage.

---

## Why use `pfeed`
You should use `pfeed` if you want to:
- Download or stream reliable, validated and **clean data** for research, backtesting, or live trading
- Get historical data (**dataframe**) or live data in standardized formats by just calling a **single** function
- **Own your data** by storing them locally using [MinIO], with the option to connect to the cloud
- Interact with different kinds of data (including {abbr}`TradFi (Traditional Finance, e.g. Interactive Brokers)`, {abbr}`CeFi (Centralized Finance, e.g. Binance)` and {abbr}`DeFi (Decentralized Finance, e.g. Uniswap)`) using a **unified interface**

---

```{tip} Note for Beginners
:class: dropdown
To start algo-trading, most people will just use [yfinance]. It is a good place to start, but it is mainly for daily data and very limited to other data resolutions, such as 1-minute data and tick data. Then, you might turn to platforms like [QuantConnect], which offers a broad range of data but is cloud-based, meaning that you have to use their machines to interact with any data and can't download the data to your local machine due to data licensing.

This is where `pfeed` comes in. If you are looking to store the data downloaded from [various sources](supported-data-sources.md) without going through the hassle of data cleaning, `pfeed` is the right tool for that. 

Unfortunately, there is no free and reliable data for {abbr}`TradFi (Traditional Finance, e.g. stocks)`, so you will need to pay for data providers like [FirstRate Data], [Databento] and [Polygon]. However, for crypto, there are some **free data** sources like [Bybit Data], [Binance Data], and [OKX Data], which could be a **good starting point** for algo-trading. You can **fiddle with crypto data first** to get a feel for algo-trading and then decide whether you want to pay for {abbr}`TradFi (Traditional Finance, e.g. stocks)` data.
```
