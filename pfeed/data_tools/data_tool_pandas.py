from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.literals import tSTORAGE

import io

import pandas as pd

from pfeed.const.enums import DataStorage, DataTool


name = DataTool.pandas


def read_parquet(paths_or_obj: list[str] | str | bytes, *args, storage: tSTORAGE, **kwargs) -> pd.DataFrame:
    if isinstance(paths_or_obj, bytes):
        obj = io.BytesIO(paths_or_obj)
        return pd.read_parquet(obj, *args, **kwargs)
    else:
        from pfeed.etl import get_filesystem
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        storage = DataStorage[storage.upper()]
        if storage not in [DataStorage.LOCAL, DataStorage.CACHE] and 'filesystem' not in kwargs:
            kwargs['filesystem'] = get_filesystem(storage)
            if storage == DataStorage.MINIO:
                paths = [path.replace('s3://', '') for path in paths]
            else:
                raise NotImplementedError(f'read_parquet() for storage {storage} is not implemented')
        return pd.read_parquet(paths, *args, **kwargs)


# def concat(dfs: list[pd.DataFrame], ignore_index: bool=True) -> pd.DataFrame:
#     return pd.concat(dfs, ignore_index=ignore_index)


# def sort_by_ts(df: pd.DataFrame) -> pd.DataFrame:
#     return df.sort_values(by='ts', ignore_index=True, ascending=True)


# def is_empty(df: pd.DataFrame) -> bool:
#     return df.empty


# def to_datetime(df: pd.DataFrame) -> pd.DataFrame:
#     # determine_timestamp_integer_unit_and_scaling_factor() is used to preserve the precision of the timestamp
#     from pandas.api.types import is_datetime64_any_dtype as is_datetime
#     from pfeed.utils.utils import determine_timestamp_integer_unit_and_scaling_factor
#     if is_datetime(df['ts']):
#         return df
#     else:
#         first_ts = df.loc[0, 'ts']
#         ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_ts)
#         df['ts'] = pd.to_datetime(df['ts'] * scaling_factor, unit=ts_unit)
#         return df
