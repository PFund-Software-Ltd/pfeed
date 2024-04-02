[yfinance]: https://github.com/ranaroussi/yfinance
[QuantConnect]: https://www.quantconnect.com
[TradFi]: https://www.techopedia.com/definition/traditional-finance-tradfi
[FirstRate Data]: https://firstratedata.com
[Bybit Data]: https://public.bybit.com
[Binance Data]: https://data.binance.vision
[OKX Data]: https://www.okx.com/data-download
[PFund.ai]: https://pfund.ai

# Overview
```{warning} 
This documentation is in the making...
```

## Background
You are interested in algo-trading and can't wait to play with some data. The go-to place for most traders is probably [yfinance], a library that provides access to historical market data from Yahoo Finance. It is a good place to start, but it is mainly for daily data and very limited to other data resolutions, such as 1-minute data and tick data. Then, you might turn to platforms like [QuantConnect], which offers a broad range of data but is cloud-based, meaning that you have to use their machines and can't download the data to your local machine.

This is where `pfeed` comes in. If you are looking to download data from various sources and store it locally, `pfeed` is the right tool for that. Unfortunately, there is no free data for [TradFi] (e.g. stocks), so you will need to pay for data providers like [FirstRate Data]. However, for crypto, there are some free data sources like [Bybit Data], [Binance Data], and [OKX Data], which could be a good starting point for algo-trading.

```{seealso}
See [Supported Data Sources](supported-data-sources.md) for more information.
```

## What is `pfeed`
PFeed (/piː fiːd/) is a data integration library tailored for algorithmic trading, 
serving as an ETL (Extract, Transform, Load) data pipeline between raw data sources and traders,
helping them in creating a **local data lake for quantitative research**.

PFeed allows traders to download historical, paper, and live data from various data sources, both free and paid,
and stores them into a local data lake using [MinIO](https://min.io/).

It is designed to be used alongside [PFund](https://github.com/PFund-Software-Ltd/pfund) — A Complete Algo-Trading Framework for Machine Learning, TradFi, CeFi and DeFi ready. Supports Vectorized and Event-Driven Backtesting, Paper and Live Trading, or as a standalone package.


## Why use `pfeed`
You should use `pfeed` if you want to:
- Join [PFund.ai]'s ecosystem, which includes:
    - **AI** (LLM) capable of analyzing your `pfund` strategies
    - A **Model Hub** for plug-and-play machine learning models
    - Strategy deployment
- Own your data and have full control over it instead of getting vendor-locked by cloud-based platforms 
- Create a local data lake for research and backtesting


<!-- 
## Table of Contents

```{tableofcontents}
``` 
-->
