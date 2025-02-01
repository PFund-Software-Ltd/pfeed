from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    
import io

import pandas as pd
import dask.dataframe as dd
from deltalake import DeltaTable

from pfeed.const.enums import DataTool


name = DataTool.dask


def read_parquet(
    paths_or_obj: list[str] | str | bytes, 
    *args, 
    filesystem: pa_fs.FileSystem | None=None, 
    storage_options: dict[str, Any] | None=None,
    **kwargs
) -> dd.DataFrame:
    if isinstance(paths_or_obj, bytes):
        obj = io.BytesIO(paths_or_obj)
        return dd.from_pandas(pd.read_parquet(obj, *args, **kwargs))
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        return dd.read_parquet(
            paths, 
            *args, 
            filesystem=filesystem, 
            storage_options=storage_options, 
            **kwargs
        )


def read_delta(delta_table: DeltaTable, **kwargs) -> dd.DataFrame:
    return dd.from_pandas(delta_table.to_pandas(**kwargs))


# def concat(dfs: list[dd.DataFrame]) -> dd.DataFrame:
#     # NOTE: in dask, using ignore_index=True in Dask does not reset the index like in Pandas.
#     # It instructs Dask to ignore the index during the concatenation process to optimize performance.
#     # use interleave_partitions=True + reset_index(drop=True) to have the same behavior as pandas's concat(ignore_index=True)
#     return dd.concat(dfs, interleave_partitions=True).reset_index(drop=True)


# def sort_by_ts(df: dd.DataFrame) -> dd.DataFrame:
#     return df.sort_values(by='ts', ascending=True).reset_index(drop=True)


# def is_empty(df: dd.DataFrame) -> bool:
#     return len(df.index) == 0


# def to_datetime(df: dd.DataFrame) -> dd.DataFrame:
#     from pfeed.utils.utils import determine_timestamp_integer_unit_and_scaling_factor
#     first_ts = df['ts'].head(1)[0]
#     ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_ts)
#     df['ts'] = dd.to_datetime(df['ts'] * scaling_factor, unit=ts_unit)
#     return df