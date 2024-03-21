"""This module contains functions for data manipulation using Polars."""
import io
import logging
import datetime

import polars as pl
from pfund.datas.resolution import Resolution


logger = logging.getLogger('pfeed')


def estimate_memory_usage(lf: pl.LazyFrame) -> float:
    """
    Returns the estimated memory usage of the given LazyFrame in GB.
    """
    num_rows: int = lf.select(pl.len()).collect().item()
    first_n_rows = min(num_rows, 1000)
    memory_usage_in_gb = lf.limit(first_n_rows).collect().to_pandas().memory_usage(deep=True).sum() / (1024 ** 3)
    estimated_memory_usage_in_gb = memory_usage_in_gb * (num_rows / first_n_rows)
    return estimated_memory_usage_in_gb


def to_parquet(df: pl.DataFrame, compression: str='snappy') -> bytes:
    """Converts a Polars DataFrame to Parquet bytes format.

    Args:
        df (pl.DataFrame): The Polars DataFrame to be converted.
        compression (optional): The compression algorithm to be used. Defaults to 'snappy'.

    Returns:
        bytes: The Parquet data in bytes format.
    """
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression=compression)
    buffer.seek(0)
    data: bytes = buffer.read()
    return data


def resample_data(data: bytes, resolution: str | Resolution, check_if_drop_last_bar: bool=False) -> bytes:
    """
    Resamples the given data based on the specified resolution.

    Args:
        data (bytes): The input data to be resampled.
        resolution (str): The desired resolution for resampling.
        check_if_drop_last_bar: Whether to check if the last bar is complete. Defaults to False.

    Returns:
        bytes: The resampled data in bytes format.
    """
    # standardize resolution by following pfund's standard, e.g. '1minute' -> '1m', which also follows Polars' standard for s/m/h/d
    if type(resolution) is not Resolution:
        resolution = Resolution(resolution)
    eresolution = repr(resolution)
    
    if type(data) is bytes:
        df = pl.read_parquet(data)
    else:
        raise TypeError(f'Invalid data type {type(data)}, expected bytes')
    
    is_tick_data = True if 'price' in df.columns else False  # REVIEW
    assert not df.is_empty(), 'data is empty'
    df = df.sort('ts')
    
    if is_tick_data:
        resample_logic = [
            pl.first('price').alias('open'),
            pl.max('price').alias('high'),
            pl.min('price').alias('low'),
            pl.last('price').alias('close'),
            pl.sum('volume'),
        ]
    else:
        resample_logic = [
            pl.first('open'),
            pl.max('high'),
            pl.min('low'),
            pl.last('close'),
            pl.sum('volume'),
        ]
    
    # NOTE: if check_if_drop_last_bar is True, meaning data_dtype == resample_data_dtype, e.g. 'daily' -> '2d'
    # polars will offset the ts column, making the first row e.g. 2024-03-01 to be 2024-02-29
    # need to set start_by = 'datapoint' to avoid this
    start_by = 'window' if not check_if_drop_last_bar else 'datapoint'
    
    resampled_df = (
        df
        .group_by_dynamic('ts', every=eresolution, closed='left', start_by=start_by)
        .agg(resample_logic)
    )
    
    # REVIEW
    if check_if_drop_last_bar:
        correct_timedelta = datetime.timedelta(seconds=resolution._value())
        final_timedelta = df.select('ts')[-1].item() - resampled_df.select('ts')[-1].item() + datetime.timedelta(seconds=resolution.timeframe.unit)
        is_drop_last_bar = (final_timedelta - correct_timedelta).total_seconds() != 0
        if is_drop_last_bar:
            logger.info(f'dropped the incomplete last bar {resampled_df.select("ts")[-1].item()} in the resampled data where {resolution=}')
            resampled_df = resampled_df.slice(0, -1)
    
    return to_parquet(resampled_df)