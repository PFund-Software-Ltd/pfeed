[yfinance]: https://github.com/ranaroussi/yfinance
[QuantConnect]: https://www.quantconnect.com
[TradFi]: https://www.techopedia.com/definition/traditional-finance-tradfi
[FirstRate Data]: https://firstratedata.com
[Databento]: https://databento.com
[Bybit Data]: https://public.bybit.com
[Binance Data]: https://data.binance.vision
[OKX Data]: https://www.okx.com/data-download
[PFund.ai]: https://pfund.ai
[MinIO]: https://min.io/
[PFund]: https://github.com/PFund-Software-Ltd/pfund

# Overview

## Background
You are interested in algo-trading and can't wait to play with some data. The go-to place for most traders is probably [yfinance], a library that provides access to historical market data from Yahoo Finance. It is a good place to start, but it is mainly for daily data and very limited to other data resolutions, such as 1-minute data and tick data. Then, you might turn to platforms like [QuantConnect], which offers a broad range of data but is cloud-based, meaning that you have to use their machines and can't download the data to your local machine.

This is where `pfeed` comes in. If you are looking to store the data downloaded from various sources without going through the hassle of data cleaning, `pfeed` is the right tool for that. Unfortunately, there is no free and reliable data for [TradFi] (e.g. stocks), so you will need to pay for data providers like [FirstRate Data] and [Databento]. However, for crypto, there are some free data sources like [Bybit Data], [Binance Data], and [OKX Data], which could be a good starting point for algo-trading.

```{seealso}
See [Supported Data Sources](supported-data-sources.md) for more information.
```

## What is `pfeed`
PFeed (/piː fiːd/) is a data pipeline for algorithmic trading, serving as a bridge between raw data sources and traders by automating the process of data collection, cleaning, transformation, and storage, loading clean data into a **local data lake for quantitative analysis**.

PFeed provides a **unified interface** for traders to download historical and live data in **standardized formats** from various data sources, both free and paid, and store them in a local data lake using [MinIO], with the option to connect to the cloud.

It is designed to be used alongside [PFund] — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready.


## Why use `pfeed`
You should use `pfeed` if you want to:
- Download reliable, validated and clean data, which is benefited from the open-source community-driven effort
- Own your data and have full control over it instead of getting vendor-locked by cloud-based platforms 
- Create a local data lake for research and backtesting
- Join [PFund.ai]'s ecosystem, which includes:
    - **AI** (LLM) capable of analyzing your trading strategies
    - **PFund Hub** for downloading trading strategies and machine learning models
    - Cloud deployment

<!--
## Table of Contents

```{tableofcontents}
``` 
-->
