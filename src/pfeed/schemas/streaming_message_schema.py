from __future__ import annotations

from typing import ClassVar

import pyarrow as pa


class StreamingMessageSchema:
    """
    pa.Schema for the standardized streaming dict produced by
    `_standardize_streaming_msg`.

    Subclasses extend `_message_fields()` with type-specific columns
    (e.g. price/volume for tick, OHLC for bar). The handler calls
    `.build(specs_schema=...)` to compose:
        common (StreamingMessage + MarketDataMessage)
        + subclass message fields
        + asset-type-specific flattened spec fields

    extra is intentionally dropped from the schema. _standardize_streaming_msg
    must drop it unconditionally; add explicit typed columns when a real use case
    appears.
    """

    # ts / msg_ts / _created_at are int64 ns on the wire (StreamingMessage); Arrow
    _COMMON_FIELDS: ClassVar[list[pa.Field]] = [
        pa.field("data_source", pa.string(), nullable=False),
        pa.field("data_category", pa.string(), nullable=False),
        pa.field("data_origin", pa.string(), nullable=False),
        pa.field("product", pa.string(), nullable=False),
        pa.field("basis", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("resolution", pa.string(), nullable=False),
        pa.field("ts", pa.int64(), nullable=False),
        pa.field("msg_ts", pa.int64(), nullable=True),
        pa.field("_created_at", pa.int64(), nullable=False),
    ]

    @classmethod
    def _message_fields(cls) -> list[pa.Field]:
        """Subclasses override with their type-specific fields."""
        return []

    @classmethod
    def build(cls, specs_schema: pa.Schema | None = None) -> pa.Schema:
        """
        Compose the full pa.Schema for a standardized streaming dict:
            common message fields + subclass-specific fields + specs.

        specs_schema is the asset-type-specific schema for the flattened
        product spec columns. MarketDataMessage carries product specs in a
        single `specs: dict[str, Any]` field; _standardize_streaming_msg
        flattens that dict into top-level columns (e.g. for options:
        strike_price, expiration_date), so the sink schema must include
        them with the correct types — which only the asset type knows.

        Pass None for asset types whose products have no specs (e.g. spot
        crypto).
        """
        fields = list(cls._COMMON_FIELDS) + cls._message_fields()
        if specs_schema is not None:
            fields.extend(specs_schema)
        return pa.schema(fields)
