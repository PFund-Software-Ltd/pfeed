from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs

import io

import pandas as pd

from pfeed.enums import DataTool


name = DataTool.pandas


def read_parquet(
    paths_or_obj: list[str] | str | bytes, 
    *args, 
    filesystem: pa_fs.FileSystem | None=None, 
    storage_options: dict[str, Any] | None=None,
    **kwargs
) -> pd.DataFrame:
    from pyarrow.lib import ArrowTypeError
    
    if isinstance(paths_or_obj, bytes):
        obj = io.BytesIO(paths_or_obj)
        return pd.read_parquet(obj, *args, **kwargs)
    else:
        paths = paths_or_obj if isinstance(paths_or_obj, list) else [paths_or_obj]
        paths = [path.replace('s3://', '') for path in paths]
        # FIXME: bug in pyarrow (see issue https://github.com/apache/arrow/issues/43574)
        # workaround: use polars to read parquet file and convert to pandas
        try:
            return pd.read_parquet(
                paths, 
                *args, 
                filesystem=filesystem, 
                storage_options=storage_options, 
                **kwargs
            )
        except ArrowTypeError:
            from pfeed.data_tools.data_tool_polars import read_parquet
            df_polars = read_parquet(
                paths, 
                *args, 
                filesystem=filesystem, 
                storage_options=storage_options, 
                **kwargs
            )
            return df_polars.collect().to_pandas()


def read_delta(
    paths: list[str] | str,
    storage_options: dict[str, Any] | None=None,
    version: int | None=None,
    **kwargs
) -> pd.DataFrame:
    from deltalake import DeltaTable
    if isinstance(paths, str):
        paths = [paths]
    dts = [DeltaTable(path, storage_options=storage_options, version=version) for path in paths]
    return pd.concat([dt.to_pandas(**kwargs) for dt in dts])


# def concat(dfs: list[pd.DataFrame], ignore_index: bool=True) -> pd.DataFrame:
#     return pd.concat(dfs, ignore_index=ignore_index)


# def sort_by_ts(df: pd.DataFrame) -> pd.DataFrame:
#     return df.sort_values(by='date', ignore_index=True, ascending=True)


# def is_empty(df: pd.DataFrame) -> bool:
#     return df.empty


# def to_datetime(df: pd.DataFrame) -> pd.DataFrame:
#     # determine_timestamp_integer_unit_and_scaling_factor() is used to preserve the precision of the timestamp
#     from pandas.api.types import is_datetime64_any_dtype as is_datetime
#     from pfeed.utils.utils import determine_timestamp_integer_unit_and_scaling_factor
#     if is_datetime(df['date']):
#         return df
#     else:
#         first_ts = df.loc[0, 'date']
#         ts_unit, scaling_factor = determine_timestamp_integer_unit_and_scaling_factor(first_ts)
#         df['date'] = pd.to_datetime(df['date'] * scaling_factor, unit=ts_unit)
#         return df
