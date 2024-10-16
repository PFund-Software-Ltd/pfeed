---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.5
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Data Feed

## What is a Data Feed?
In pfeed, data feeds are objects that serve as high-level interfaces designed for straightforwrad interaction with the data sources. The object `feed` in the following setup code snippet is an example of a data feed object:
```{code-block} python
feed = pe.BybitFeed()
```

To print out the supported data feeds in pfeed, you can use the following code snippet:
```{code-block} python
from pfeed.const.common import SUPPORTED_DATA_FEEDS
from pprint import pprint

pprint(SUPPORTED_DATA_FEEDS)
```