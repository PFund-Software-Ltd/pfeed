[ACID]: https://en.wikipedia.org/wiki/ACID
[Parquet]: https://parquet.apache.org/
[Spark]: https://spark.apache.org/

# Delta Lake

## What is Delta Lake?
Delta Lake is open source software that **extends [Parquet]** data files with a file-based transaction log for **[ACID] transactions** and scalable metadata handling.

In other words, Delta Lake makes a **[data lake](./data_lake.md) behave like a database**.


## Why use Delta Lake?
There are two key reasons to use Delta Lake in `pfeed`:
1. **ACID Transactions**: Writing streaming data to a data lake is **more efficient and safer** with ACID guarantees. Without ACID, **writing new rows to Parquet files in parallel** can lead to **inconsistencies and corruption**
2. **Versioned Data & Time Travel**: 
Delta Lake allows you to **version your data** and **time travel** to a specific version. This is useful for reproducibility, debugging, and preventing accidental data loss.

```{caution} Which Delta Lake?
:class: dropdown
There are two GitHub repositories for Delta Lake:
- [delta](https://github.com/delta-io/delta) - The original Delta Lake project, deeply integrated with [Spark]. It has the most complete feature set.
- [delta-rs](https://github.com/delta-io/delta-rs) - A Rust implementation of Delta Lake. While it has fewer features, it enables integration with data tools such as `polars` and `dask`.

`pfeed` uses the `delta-rs` implementation.
```

To see how to use Delta Lake in `pfeed`, please refer to [Storage](../tutorials/storage.ipynb) and [Integration with Delta Lake](../integrations/deltalake.ipynb).

For more details, please refer to the [Delta Lake documentation](https://delta-io.github.io/delta-rs/why-use-delta-lake/).