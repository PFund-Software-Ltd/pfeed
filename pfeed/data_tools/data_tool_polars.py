from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing.literals import tSTORAGE
    
import os

import polars as pl

from pfeed.const.enums import DataStorage, DataTool


name = DataTool.polars


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSTORAGE, **kwargs) -> pl.DataFrame | pl.LazyFrame:
    if isinstance(paths_or_obj, bytes):
        obj = paths_or_obj
        return pl.read_parquet(obj, *args, **kwargs)
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        storage = DataStorage[storage.upper()]
        if storage not in [DataStorage.LOCAL, DataStorage.CACHE] and 'storage_options' not in kwargs:
            if storage == DataStorage.MINIO:
                from pfeed.storages.minio_storage import MinioStorage
                kwargs['storage_options'] = {
                    "endpoint_url": MinioStorage.create_endpoint(),
                    "access_key_id": os.getenv('MINIO_ROOT_USER', 'pfunder'),
                    "secret_access_key": os.getenv('MINIO_ROOT_PASSWORD', 'password'),
                }
            else:
                raise NotImplementedError(f"read_parquet() for storage {storage} is not implemented")
        return pl.scan_parquet(paths, *args, **kwargs)


# def concat(dfs: list[pl.DataFrame | pl.LazyFrame]) -> pl.DataFrame | pl.LazyFrame:
#     return pl.concat(dfs)


# def sort_by_ts(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
#     return df.sort(by='ts', descending=False)


# def is_empty(df: pl.DataFrame | pl.LazyFrame) -> bool:
#     if isinstance(df, pl.LazyFrame):
#         return df.limit(1).collect().is_empty()
#     return df.is_empty()


# def to_datetime(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
#     from pfeed.utils.utils import determine_timestamp_integer_unit_and_scaling_factor
#     if df['ts'].dtype == pl.Datetime:
#         return df
#     else:
#         if isinstance(df, pl.LazyFrame):
#             first_ts = df.select(pl.col("ts").first()).collect().item()
#         else:
#             first_ts = df[0, "ts"]
#         ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_ts)
#         return df.with_columns(
#             (pl.col("ts") * scaling_factor).cast(pl.Datetime(time_unit=ts_unit))
#         )
