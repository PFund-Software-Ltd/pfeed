# Installation

[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/vqpS94tpdp)
[![X](https://img.shields.io/twitter/follow/pfund_ai?style=social)](https://x.com/pfund_ai)
[![Downloads](https://static.pepy.tech/badge/pfeed/month)](https://pepy.tech/project/pfeed)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg?cacheSeconds=300)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed?cacheSeconds=300)
[![afterpython](https://afterpython.org/shield.svg)](https://afterpython.org)

## Standard

Install `pfeed` with your data source, e.g. Bybit, and [Ray] for parallel processing (recommended):
```bash
pip install "pfeed[bybit,ray]"
```

---

## Windows: enable long paths

:::{important}
Do this once before using pfeed on Windows, or storing data fails with `[WinError 206] The filename or extension is too long`.
:::

Windows limits paths to 260 characters by default. pfeed stores data in nested folders (e.g. `env=BACKTEST/data_layer=CLEANED/data_domain=MARKET_DATA/...`), which together with your data folder can go over that limit.

Run this once in PowerShell **as Administrator** (right-click PowerShell → *Run as administrator*):
```powershell
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```
Then open a new terminal (restart Windows if the error persists). Python supports long paths, so nothing else is needed.

---

## Advanced

Pick only the extras you need:

| Extra | Adds |
| --- | --- |
| `bybit` | the [pfeed-bybit] data source |
| `ray` | parallel processing with [Ray] |
| `prefect` | workflow orchestration with [Prefect] |

Combine them in one command:
```bash
pip install "pfeed[bybit,ray,prefect]"
```

[pfeed-bybit]: https://github.com/PFund-Software-Ltd/pfeed-bybit
[Prefect]: https://www.prefect.io/
[Ray]: https://www.ray.io/
