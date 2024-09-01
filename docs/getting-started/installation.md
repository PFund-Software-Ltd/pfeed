# Installation

![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social)
![PyPI downloads](https://img.shields.io/pypi/dm/pfeed)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)
[![Jupyter Book Badge](https://raw.githubusercontent.com/PFund-Software-Ltd/pfeed/main/docs/images/jupyterbook.svg)](https://jupyterbook.org)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)


## Using [Poetry](https://python-poetry.org) (Recommended)
```bash
# [RECOMMENDED]: Download data (e.g. Bybit and Yahoo Finance) + Data tools (e.g. pandas, polars) + Data storage (e.g. MinIO) + Boosted performance (e.g. Ray)
poetry add "pfeed[all]"

# [Download data + Data tools + Data storage]
poetry add "pfeed[df,data]"

# [Download data + Data tools]
poetry add "pfeed[df]"

# [Download data only]:
poetry add pfeed

# update to the latest version:
poetry update pfeed
```


## Using Pip

```bash
# same as above, you can choose to install "pfeed[all]", "pfeed[df,data]", "pfeed[df]" or "pfeed"
pip install "pfeed[all]"

# install the latest version:
pip install -U pfeed
```


## Checking your installation
```bash
$ pfeed --version
```