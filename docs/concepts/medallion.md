# Medallion Architecture

The Medallion Architecture organizes data storage into three layers:

| Layer | Description | Term Used in PFeed |
| --- | --- | --- |
| **Bronze** | Raw & Unprocessed data | `raw` |
| **Silver** | Cleaned & Processed Data | `cleaned` (**default** in download) |
| **Gold** | Curated & Business-Ready Data | `curated` |

1. if `data_layer=raw`, downloaded data is stored as-is, without transformations.
2. if `data_layer=cleaned`, data is transformed into a standardized format before storage.
3. if `data_layer=curated`, data is processed with custom transformations to prepare it for business use.


See [Storage](../tutorials/storage.ipynb) for more details about setting data layers.