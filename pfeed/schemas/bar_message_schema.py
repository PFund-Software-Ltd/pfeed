import pyarrow as pa

from pfeed.schemas.streaming_message_schema import StreamingMessageSchema


class BarMessageSchema(StreamingMessageSchema):
    """pa.Schema for standardized BarMessage dicts."""

    @classmethod
    def _message_fields(cls) -> list[pa.Field]:
        return [
            pa.field("start_ts", pa.timestamp("ns"), nullable=False),
            pa.field("end_ts", pa.timestamp("ns"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("is_incremental", pa.bool_(), nullable=False),
        ]
