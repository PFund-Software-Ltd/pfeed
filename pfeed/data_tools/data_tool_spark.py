from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tSTORAGE

import os

from pyspark.sql import DataFrame as SparkDataFrame, functions as F, SparkSession

from pfeed.enums import DataStorage, DataTool


name = DataTool.spark


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSTORAGE, **kwargs) -> SparkDataFrame:
    # FIXME: this is hard-coded, let users pass in spark session or builder somewhere?
    spark_builder = SparkSession.builder \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        
    if isinstance(paths_or_obj, bytes):
        from pfeed.data_tools.data_tool_pandas import read_parquet as read_parquet_pandas
        pandas_df = read_parquet_pandas(paths_or_obj, storage=storage)
        return spark_builder.getOrCreate().createDataFrame(pandas_df)
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        paths = [str(path).replace('s3://', 's3a://') for path in paths]
        storage: DataStorage = DataStorage[storage.upper()]
        spark = spark_builder.getOrCreate()
        if storage not in [DataStorage.LOCAL, DataStorage.CACHE]:
            if storage == DataStorage.MINIO:
                from pfeed.storages.minio_storage import MinioStorage
                endpoint = MinioStorage.create_endpoint()
                spark = spark_builder \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if endpoint.startswith('https://') else "false") \
                    .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
                    .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ROOT_USER', 'pfunder')) \
                    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_ROOT_PASSWORD', 'password')) \
                    .getOrCreate()
            else:
                raise NotImplementedError(f"read_parquet() for storage {storage} is not implemented")
        return spark.read.parquet(*paths, *args, **kwargs)


# def concat(dfs: list[SparkDataFrame]) -> SparkDataFrame:
#     from functools import reduce
#     return reduce(SparkDataFrame.union, dfs)


# def sort_by_ts(df: SparkDataFrame) -> SparkDataFrame:
#     return df.orderBy('date', ascending=True)


# def is_empty(df: SparkDataFrame) -> bool:
#     return df.rdd.isEmpty()


# def to_datetime(df: SparkDataFrame) -> SparkDataFrame:
#     return df.withColumn('date', F.to_timestamp(F.col('date')))