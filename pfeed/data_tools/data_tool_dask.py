from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.literals import tSTORAGE
    
import io

import pandas as pd
import dask.dataframe as dd

from pfeed.const.enums import DataStorage, DataTool


name = DataTool.dask


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSTORAGE, **kwargs) -> dd.DataFrame:
    if isinstance(paths_or_obj, bytes):
        obj = io.BytesIO(paths_or_obj)
        return dd.from_pandas(pd.read_parquet(obj, *args, **kwargs))
    else:
        from pfeed.etl import get_filesystem
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        storage = DataStorage[storage.upper()]
        if storage not in [DataStorage.LOCAL, DataStorage.CACHE] and 'filesystem' not in kwargs:
            kwargs['filesystem'] = get_filesystem(storage)
        return dd.read_parquet(paths, *args, **kwargs)


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