[DuckDB]: https://duckdb.org/
[Polars]: https://pola.rs/
[Spark]: https://spark.apache.org/

# ETL vs ELT
::::{aside}
:::{glossary}
ETL
: Extract, Transform, Load. A process of extracting data from a source, transforming it into a desired format, and loading it into a destination.
:::

:::{glossary}
ELT
: Extract, Load, Transform. A process of extracting data from a source, loading it into a destination, and transforming it into a desired format.
:::
::::

Traditionally, **ETL (Extract, Transform, Load)** processes data before loading it into a destination, such as a database. However, this approach has some drawbacks:
- **Upfront Transformation**: ETL transforms data before loading, limiting flexibility for future transformations or evolving analytical needs.
- **Limited Data Exploration**: Since ETL transformed data first, raw data is discarded, making it harder to explore historical data for new insights.


Empowered by growing computational power, **ELT (Extract, Load, Transform)** is becoming the standard for data processing. `pfeed` leverages this by storing raw data in **columnar formats**, allowing you to perform complex transformations with high performance and low costs using tools like [DuckDB], [Polars] and [Spark].


```{important} Takeaway
`pfeed` follows the ELT pattern to extract raw data from sources, store them in a local data lake, leaving the transformation part to you for your research.
```
