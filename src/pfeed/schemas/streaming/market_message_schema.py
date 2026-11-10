from __future__ import annotations

from typing import ClassVar, Final

import pyarrow as pa

from pfeed.schemas.market_data_schema import PRODUCT

# column names of streamed market data, typed as str for code referring to them
RECEIVED_AT: Final = "received_at"
SERVER_TS: Final = "server_ts"
CREATED_AT: Final = "created_at"


class MarketMessageSchema:
    """pa.Schema for the rows of streamed market data, one row per data point in Message["data"].

    A row has the envelope fields below, then the data point's fields (e.g. Bar or Tick in pfund_blocks).
    Fields the dataset key already has (e.g. data_source in its namespace, resolution in its name)
    and constants of the product (e.g. symbol, specs) are not stored per row.

    Timestamps are int64 ns since epoch in the data point, they are stored as UTC ns timestamps.
    """

    _COMMON_FIELDS: ClassVar[list[pa.Field]] = [
        # the partition column, see MarketDataHandler._partition_prefix()
        pa.field(PRODUCT, pa.string(), nullable=False),
        pa.field(
            RECEIVED_AT, pa.timestamp("ns", tz="UTC"), nullable=False
        ),  # Message["received_at"]
    ]
    # BaseDataPoint fields
    _DATA_POINT_FIELDS: ClassVar[list[pa.Field]] = [
        pa.field(SERVER_TS, pa.timestamp("ns", tz="UTC"), nullable=True),
        pa.field(CREATED_AT, pa.timestamp("ns", tz="UTC"), nullable=False),
    ]

    @classmethod
    def _message_fields(cls) -> list[pa.Field]:
        """Subclasses override with their data point's own fields."""
        return []

    @classmethod
    def build(cls) -> pa.Schema:
        return pa.schema(
            [*cls._COMMON_FIELDS, *cls._message_fields(), *cls._DATA_POINT_FIELDS]
        )
