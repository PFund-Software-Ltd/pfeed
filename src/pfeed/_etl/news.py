"""
ETL for news data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import polars as pl


def standardize_columns(
    df: pl.LazyFrame, product: BaseProduct | None = None
) -> pl.LazyFrame:
    """Standardizes the columns of a DataFrame.
    Adds column 'product'.
    """
    return df.with_columns(
        pl.lit(product.name if product else None).alias("product"),
    )


def filter_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    """Filter out unnecessary columns from raw data."""
    cols = df.collect_schema().names()
    standard_cols = ["date", "product", "title", "content", "publisher", "url"]
    extra_cols = ["author", "exchange"]
    for extra_col in extra_cols:
        if extra_col in cols:
            standard_cols.append(extra_col)
    return df.select(standard_cols)


def organize_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    """Moves 'date', 'product', 'publisher', 'title', 'content', 'url' to the leftmost side."""
    left_cols = ["date", "product", "publisher", "title", "content", "url"]
    extra_cols = ["author", "exchange"]
    current_cols = df.collect_schema().names()
    for extra_col in extra_cols:
        if extra_col in current_cols:
            left_cols.append(extra_col)
    target_cols = left_cols + [c for c in current_cols if c not in left_cols]
    if current_cols == target_cols:
        return df
    return df.select(target_cols)
