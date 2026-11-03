from typing import Final

import pyarrow as pa

from pfeed.schemas.bar_data_schema import CLOSE, HIGH, LOW, OPEN
from pfeed.schemas.market_data_schema import VOLUME
from pfeed.schemas.streaming.market_message_schema import MarketMessageSchema

# column names of streamed bars, typed as str for code referring to them
START_TS: Final = "start_ts"
END_TS: Final = "end_ts"
UPDATED_TS: Final = "updated_ts"
IS_CLOSED: Final = "is_closed"


class BarMessageSchema(MarketMessageSchema):
    """pa.Schema for streamed pfund_blocks Bar rows."""

    @classmethod
    def _message_fields(cls) -> list[pa.Field]:
        return [
            pa.field(START_TS, pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(END_TS, pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(UPDATED_TS, pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(OPEN, pa.float64(), nullable=False),
            pa.field(HIGH, pa.float64(), nullable=False),
            pa.field(LOW, pa.float64(), nullable=False),
            pa.field(CLOSE, pa.float64(), nullable=False),
            pa.field(VOLUME, pa.float64(), nullable=False),
            pa.field(IS_CLOSED, pa.bool_(), nullable=False),
        ]
