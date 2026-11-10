# CONTRIBUTING
`pfeed` is an ambitious project with a simple goal: **one unified API to get any financial data, from any data source**, as easily as possible.

With enough contributors, `pfeed` can end the need for duplicate libraries that each wrap the same data providers.

Conceptually, you can think of `pfeed` as a **meta-data provider** that unifies access to various financial data sources.

No single team can cover every data source, so `pfeed` is built to grow through plugins: the most valuable way to contribute is to bring a new data source to `pfeed`.

---

## Data Source Plugins

Data sources are not built into `pfeed`. Each one ships as its own package, a **plugin**, named `pfeed-xxx` by convention. A plugin registers its data source with `pfeed` when installed, so it works with `pfeed` without any change to `pfeed` itself.

Official plugins, maintained by the PFund team:
- [pfeed-bybit](https://github.com/PFund-Software-Ltd/pfeed-bybit)
- [pfeed-hyperliquid](https://github.com/PFund-Software-Ltd/pfeed-hyperliquid)

Official plugins are installed as extras of `pfeed`, which pin a version verified to work with it:

```bash
pip install "pfeed[bybit]"
```

Anyone can build and publish a plugin for the data source they need. Using one is the same as using any other Python package:

```bash
pip install pfeed-xxx
```

```python
from pfeed_xxx import SomeDataSource

source = SomeDataSource()
feed = source.market_feed
```

---

## Building a Plugin

🚧 Coming later: `pfeed` will provide a template for `pfeed-xxx` packages, along with a step-by-step guide on building a plugin for your data source. **If you are a data provider, this will be the way to make your data available to `pfeed` users.**

---

## Other Ways to Contribute

Not building a plugin? You can still help by:
- Reporting bugs in [GitHub Issues](https://github.com/PFund-Software-Ltd/pfeed/issues)
- Suggesting features and sharing feedback in [GitHub Discussions](https://github.com/PFund-Software-Ltd/pfeed/discussions)
