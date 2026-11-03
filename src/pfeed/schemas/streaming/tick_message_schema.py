from typing import Final

import pyarrow as pa

from pfeed.schemas.market_data_schema import VOLUME
from pfeed.schemas.streaming.market_message_schema import MarketMessageSchema
from pfeed.schemas.tick_data_schema import PRICE, SIDE

# column names of streamed ticks, typed as str for code referring to them
TS: Final = "ts"


class TickMessageSchema(MarketMessageSchema):
    """pa.Schema for streamed pfund_blocks Tick rows."""

    @classmethod
    def _message_fields(cls) -> list[pa.Field]:
        return [
            pa.field(TS, pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(PRICE, pa.float64(), nullable=False),
            pa.field(VOLUME, pa.float64(), nullable=False),
            # None if the data source doesn't provide it, int8 as in batch data's side column
            pa.field(SIDE, pa.int8(), nullable=True),
        ]
