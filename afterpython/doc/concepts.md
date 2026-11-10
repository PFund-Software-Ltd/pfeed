# Concepts

::::{aside}

:::{glossary}
Data Lake
: A data lake is a centralized repository that allows you to store all your structured and unstructured data in their **raw formats** at any scale.
:::

:::{glossary}
Data Warehouse
: A data warehouse is a system for storing structured data with pre-defined schemas so that it can be used for reporting and analysis out-of-the-box.
:::

:::{glossary}
Data Lakehouse
: A data lakehouse combines the cheap, open file storage of a data lake with the schemas and **ACID transactions** of a data warehouse.
:::

:::{glossary}
DuckLake
: DuckLake is an open lakehouse format from the DuckDB team. Data is stored as **Parquet** files, while metadata (schemas, snapshots) is kept in a SQL database (e.g. DuckDB, SQLite, PostgreSQL).
:::

:::{glossary}
Medallion Architecture
: A data design pattern that organizes data into layers of increasing quality: **Bronze** (`RAW`), **Silver** (`CLEANED`) and **Gold** (`CURATED`).
:::

:::{glossary}
ETL
: Extract, Transform, Load. Data is transformed **before** it is loaded into the destination.
:::

:::{glossary}
ELT
: Extract, Load, Transform. Data is loaded into the destination **first** and transformed later, when needed.
:::

::::


## Local Data Lakehouse

`pfeed` is designed for **scalability**, allowing traders to store massive amounts of data, such as orderbook data and tick data, across multiple asset classes. Imagine being able to store all the data for stocks, futures, and options as a retail trader.
> To put this into perspective, the BTC-USDT perpetual contract on Bybit has generated 1 billion+ tick data records from 2020 to the present. This is the scale of data we are dealing with.

To handle this, `pfeed` stores data in a {term}`DuckLake`. Whether it's `RAW` or `CLEANED` data, it's all just Parquet files underneath, so the same data lake can live on your local disk or be moved to cloud object storage (e.g. AWS S3) when needed. This way, you can start small with **local storage at no cost** and easily **scale up to the cloud** when your data needs grow. Of course, cloud storage will incur costs, but it is much more **cost-effective** than storing large volumes of data in cloud databases.

```{note}
:class: dropdown
The lakehouse is optional: if you don't need what DuckLake adds on top (e.g. ACID transactions, snapshots), you can simply store your data as plain Parquet files.
```

---

## Medallion Architecture

`pfeed` organizes stored data into three layers, following the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture). Each layer is defined by **who produced the data**:

| Layer | Term in `pfeed` | Produced by | Description |
| --- | --- | --- | --- |
| **Bronze** | `RAW` | the data vendor | data stored as-is, without any transformations |
| **Silver** | `CLEANED` (**default**) | `pfeed` | data transformed into `pfeed`'s standardized format |
| **Gold** | `CURATED` | you | `CLEANED` data further transformed with your own transformations |

You choose the layer with `data_layer`: `download()` accepts `'RAW'` or `'CLEANED'`, and `CLEANED` data can be stored as `'CURATED'` after applying your own transformations, e.g. `.transform(...).load(io, data_layer='CURATED')`.

---

## ETL vs ELT

ETL and ELT differ in **when** data is transformed, and therefore in **what you keep**. In `pfeed`, this comes down to which {term}`Medallion Architecture` layer you store downloaded data in.

### ETL: store `CLEANED` data

By default, `pfeed` transforms the vendor data into its standardized format **before** storing it. This is ETL.

- **Pro**: one copy of the data, ready to use.
- **Con**: the vendor's original data is not kept. Any question that needs it later can't be answered from your storage; you'd have to download it again, assuming the vendor still provides it.

### ELT: store `RAW` data

With `data_layer='RAW'`, `pfeed` stores the vendor data **as-is**. This is ELT: `pfeed` handles the "E" and the "L", and the "T" is up to you, whenever you need it. For example:

- **`pfeed`'s default transformations** → `CLEANED` data, an opinionated standardization of the vendor data.
- **Your own transformations** (features, resampling, joins, etc.) → `CURATED` data.

- **Pro**: nothing is lost. Since the vendor's original data is kept, you can always transform it again, e.g. when `pfeed`'s cleaning improves or your research needs change.
- **Con**: every transformed version you store is an extra copy of the same data on top of the `RAW` data.
