'''
ETL for market data.
'''
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import polars as pl

from pfund.datas.resolution import Resolution
from pfund.datas.timeframe import Timeframe


def standardize_columns(df: pl.LazyFrame, product: BaseProduct, resolution: Resolution) -> pl.LazyFrame:
    """Standardizes the columns of a DataFrame.
    Adds columns 'product', 'resolution'
    """
    return df.with_columns(
        pl.lit(product.name).alias('product'),
        pl.lit(repr(resolution)).alias('resolution'),
    )


def filter_columns(df: pl.LazyFrame, product: BaseProduct) -> pl.LazyFrame:
    """Filter out unnecessary columns from raw data."""
    cols = df.collect_schema().names()
    is_tick_data = 'price' in cols
    if is_tick_data:
        standard_cols = ['date', 'product', 'resolution', 'side', 'volume', 'price']
    else:
        standard_cols = ['date', 'product', 'resolution', 'open', 'high', 'low', 'close', 'volume']
    extra_cols: list[str] = []
    if product.is_stock() or product.is_etf():
        extra_cols.extend(['dividends', 'splits'])
    for extra_col in extra_cols:
        if extra_col in cols:
            standard_cols.append(extra_col)
    return df.select(standard_cols)


def organize_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    '''Moves 'date', 'product', 'resolution' to the leftmost side.'''
    left_cols = ['date', 'product', 'resolution']
    current_cols = df.collect_schema().names()
    target_cols = left_cols + [c for c in current_cols if c not in left_cols]
    if current_cols == target_cols:
        return df
    return df.select(target_cols)


def resample_data(
    df: pl.LazyFrame,
    resolution: str | Resolution,
    product: BaseProduct,
    offset: str | None = None,
) -> pl.LazyFrame:
    '''Resamples the input lazyframe based on the target resolution.
    Args:
        df: The input lazyframe to be resampled.
        resolution: The target resolution to resample the data to.
        offset: Optional polars duration string (e.g., '30m') to shift the resampling window.
               For example, with '1h' resolution and '30m' offset, timestamps will be XX:30:00.
    Returns:
        The resampled lazyframe.
    '''
    if isinstance(resolution, str):
        resolution = Resolution(resolution)

    cols = df.collect_schema().names()
    if 'resolution' in cols:
        df_resolution_str = df.select(pl.col('resolution').first()).collect().item()
        df_resolution = Resolution(df_resolution_str)
        is_resample_required = resolution < df_resolution
        if not is_resample_required:
            return df

    df = filter_columns(df, product)

    eresolution = {
        Timeframe.MINUTE: '1m',
        Timeframe.DAY: '1d',
        Timeframe.WEEK: '1w',
        Timeframe.MONTH: '1mo',
        Timeframe.YEAR: '1y',
    }.get(resolution.timeframe, f'1{resolution.timeframe.canonical}')

    cols = df.collect_schema().names()
    is_tick_data = 'price' in cols

    aggs: list[pl.Expr] = []
    if is_tick_data:
        aggs.extend([
            pl.col('price').first().alias('open'),
            pl.col('price').max().alias('high'),
            pl.col('price').min().alias('low'),
            pl.col('price').last().alias('close'),
            pl.col('volume').sum().alias('volume'),
        ])
    else:
        aggs.extend([
            pl.col('open').first().alias('open'),
            pl.col('high').max().alias('high'),
            pl.col('low').min().alias('low'),
            pl.col('close').last().alias('close'),
            pl.col('volume').sum().alias('volume'),
        ])
    for c in ('product', 'resolution'):
        if c in cols:
            aggs.append(pl.col(c).first().alias(c))
    if 'dividends' in cols:
        aggs.append(pl.col('dividends').sum().alias('dividends'))
    if 'splits' in cols:
        aggs.append(pl.col('splits').product().alias('splits'))

    resampled = (
        df
        .sort('date')
        .group_by_dynamic(
            'date',
            every=eresolution,
            offset=offset,
            label='left',
            closed='left',
        )
        .agg(aggs)
        .with_columns(pl.lit(repr(resolution)).alias('resolution'))
    )
    return resampled
