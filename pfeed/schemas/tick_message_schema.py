import pyarrow as pa

from pfeed.schemas.streaming_message_schema import StreamingMessageSchema


class TickMessageSchema(StreamingMessageSchema):
    """pa.Schema for standardized TickMessage dicts."""

    @classmethod
    def _message_fields(cls) -> list[pa.Field]:
        return [
            pa.field("index", pa.int64(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            # volume may be None for low-quality sources (e.g. yahoo finance)
            pa.field("volume", pa.float64(), nullable=True),
        ]
