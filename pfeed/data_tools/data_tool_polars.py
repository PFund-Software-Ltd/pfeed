from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from deltalake import DeltaTable
    
import polars as pl

from pfeed.const.enums import DataTool


name = DataTool.polars


def read_parquet(
    paths_or_obj: list[str] | str | bytes, 
    *args, 
    filesystem: pa_fs.FileSystem | None=None,  # for consistency with other data tools
    storage_options: dict[str, Any] | None=None,
    **kwargs
) -> pl.DataFrame | pl.LazyFrame:
    if isinstance(paths_or_obj, bytes):
        obj = paths_or_obj
        return pl.read_parquet(obj, *args, **kwargs)
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        return pl.scan_parquet(paths, *args, storage_options=storage_options, **kwargs)


def read_delta(delta_table: DeltaTable, **kwargs) -> pl.LazyFrame:
    return pl.scan_delta(delta_table, **kwargs)
    

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
