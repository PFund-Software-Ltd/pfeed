[MinIO]: https://min.io/

# Data Lake
::::{aside}
:::{glossary}
MinIO
: MinIO is an object storage system that is Open Source, Amazon S3 compatible.
:::

:::{glossary}
Data Lake
: A data lake is a centralized repository that allows you to store all your structured and unstructured data in their **raw formats** at any scale.
:::

:::{glossary}
Data Warehouse
: A data warehouse is a system (mostly for enterprise use) for storing structured data with pre-defined schemas so that it can be used for reporting and analysis out-of-the-box.
:::

:::{glossary}
Data Lakehouse
: A data lakehouse is a modern data architecture that combines the best features of data lakes and data warehouses to create a single platform for storing and analyzing data.
:::
::::


`pfeed` is designed for **scalability**, allowing traders to store massive amounts of data, such as orderbook data and tick data, across multiple asset classes. Imagine being able to store all the data for stocks, futures, and options as a retail trader.
> To put this into perspective, the BTC-USDT perpetual contract on Bybit has generated 1 billion+ tick data records from 2020 to the present. This is the scale of data we are dealing with.

To handle this, `pfeed` uses [MinIO] as a local data lake for storing **raw data**. This setup allows you to seamlessly move the data lake to the cloud (e.g. AWS S3) if needed. This way, you can start small with **local stoarge at no cost** and easily **scale up to the cloud** when your data needs grow. Of course, using cloud storage services will incur costs, but it is much more **cost-effective** than storing large volumes of data in cloud databases.


```{important} Takeaway
Using `pfeed`, you can store all the raw data locally first and apply any transformations you want on the raw data for your research and trading later, with the ability to scale up at any time.
```
