'''
ETL for news data.
'''
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct

import pandas as pd


def standardize_columns(df: pd.DataFrame, product: BaseProduct | None=None) -> pd.DataFrame:
    from pfeed._etl.base import standardize_date_column
    df['product'] = product.name if product else None
    if 'symbol' not in df.columns and product.symbol:
        df['symbol'] = product.symbol
    df = standardize_date_column(df)
    return df


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    standard_cols = ['date', 'product', 'title', 'content', 'publisher', 'url']
    df_cols = df.columns
    extra_cols = ['author', 'exchange', 'symbol']
    for extra_col in extra_cols:
        if extra_col in df_cols:
            standard_cols.append(extra_col)
    df = df.loc[:, standard_cols]
    return df


def organize_columns(df: pd.DataFrame) -> pd.DataFrame:
    left_cols = ['date', 'product', 'publisher', 'title', 'content', 'url']
    extra_cols = ['author', 'exchange', 'symbol']
    df_cols = df.columns
    for extra_col in extra_cols:
        if extra_col in df_cols:
            left_cols.append(extra_col)
    df = df.reindex(left_cols + [col for col in df.columns if col not in left_cols], axis=1)
    return df
