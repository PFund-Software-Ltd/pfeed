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
