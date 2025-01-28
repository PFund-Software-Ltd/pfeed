# Installation

[![Twitter Follow](https://img.shields.io/twitter/follow/pfund_ai?style=social)](https://x.com/pfund_ai)
![GitHub stars](https://img.shields.io/github/stars/PFund-Software-Ltd/pfeed?style=social)
![PyPI downloads](https://img.shields.io/pypi/dm/pfeed)
[![PyPI](https://img.shields.io/pypi/v/pfeed.svg)](https://pypi.org/project/pfeed)
![PyPI - Support Python Versions](https://img.shields.io/pypi/pyversions/pfeed)
<!-- [![Jupyter Book Badge](https://raw.githubusercontent.com/PFund-Software-Ltd/pfeed/main/docs/images/jupyterbook.svg)](https://jupyterbook.org) -->
<!-- [![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/) -->


::::{tab-set}
:::{tab-item} Standard
:sync: tab1
```bash
# [RECOMMENDED]: Core Features, including Minio, Deltalake, Ray, etc.
pip install -U "pfeed[core]"
```
```bash
# Minimal Features
pip install -U "pfeed"
```


:::
:::{tab-item} Advanced
:sync: tab2
| Command                             | Installed Features                                   |
| ----------------------------------- | ---------------------------------------------------- |
| `pip install -U "pfeed[core]"`      | Core Features, including Minio, Deltalake, Ray, etc. |
| `pip install -U "pfeed[dask]"`      | Data Tools Dask                                      |
| `pip install -U "pfeed[prefect]"`   | Workflow Orchestration Framework Prefect             |
| `pip install -U "pfeed[bytewax]"`   | Stream Processing Framework Bytewax                  |
| `pip install -U "pfeed[kafka]"`     | Confluent's Kafka Python Client                      |
| `pip install -U "pfeed[databento]"` | Data Source Databento                              |
| `pip install -U "pfeed[polygon]"`   | Data Source Polygon                                  |
<!-- | `pip install -U "pfeed[spark]"`   | Data Tools PySpark | -->

---

**Combinations** \
You can create a combination of features based on your needs.
```bash
# e.g. if you only want to use prefect dataflows and databento's data
pip install -U "pfeed[core,prefect,databento]"
```
:::
::::


<!-- ````{important} WASM Usage
:class: dropdown
Since {abbr}`WASM (Web Assembly)` is becoming more mature, `pfeed` has been designed to support WASM usage as well. \
In other words, you can use `pfeed` in the browser on websites such as [Quadratic](https://quadratichq.com) (see [](../integrations/quadratic)). This is possible because `pfeed` is a **Pure Python package**.

You may try to install `pfeed` on [JupyterLite](https://jupyter.org/try-jupyter/lab/) with the following command:
```{code-block} python
import micropip
await micropip.install("pfeed")  # "pfeed[all]" or "pfeed[core]" will fail

import pfeed as pe
pe.__version__
```

```{caution} Limitations
In WASM, you can only install `pfeed`, not `pfeed[...]` or any other combinations since they include non-pure python packages.
```
```` -->