from typing import Final

import pandera.polars as pa

from pfeed.schemas.bar_data_schema import BarDataSchema
from pfeed.schemas.tick_data_schema import TickDataSchema

# column names of cleaned stock data, typed as str for code referring to them
DIVIDENDS: Final = "dividends"  # cash dividend per share on the ex-dividend date, 0 = none
SPLITS: Final = "splits"  # split ratio on the split date, e.g. 4.0 for a 4-for-1 split, 1.0 = none


class StockDataSchemaMixin(pa.DataFrameModel):
    """Stock-specific columns, combined with a data type schema (tick/bar) below.

    Fields are optional because whether they exist depends on the source and the resolution
    (e.g. Yahoo Finance's daily bars have them, an intraday source may not);
    when present, they must be valid.
    """

    # `| None` makes the column optional (pandera: required=False), its values stay non-nullable
    dividends: float | None = pa.Field(alias=DIVIDENDS, ge=0)
    splits: float | None = pa.Field(alias=SPLITS, gt=0)


class StockBarDataSchema(StockDataSchemaMixin, BarDataSchema):
    pass


class StockTickDataSchema(StockDataSchemaMixin, TickDataSchema):
    pass
