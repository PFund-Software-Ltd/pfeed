import time
from typing import Annotated, Any

from msgspec import Meta, Struct, field, structs

from pfeed.enums import DataCategory, DataSource


class StreamingMessage(
    Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
    array_like=False,  # NOTE: setting array_like=True will boost performance by 1.5-2x
    gc=True,  # OPTIMIZE: consider setting gc=False for performance boost (but then you can't use lists, dicts etc.)
    tag=True,
):
    data_source: DataSource
    data_category: DataCategory
    data_origin: str = ""
    specs: dict[
        str, Any
    ]  # product specifications, e.g. for options, specs are strike_price, expiration, etc.
    # timestamps below are int64 ns since epoch; gt-bound catches passing seconds by mistake
    # NOTE: msg_ts vs ts, e.g. for candlesticks, "msg_ts" is the timestamp of the message sent by the exchange, "ts" is the timestamp of the candlestick
    ts: Annotated[int, Meta(gt=10_000_000_000)]
    msg_ts: Annotated[int, Meta(gt=10_000_000_000)] | None = None
    _created_at: int = field(default_factory=time.time_ns)
    extra_data: dict[str, Any] = field(default_factory=dict)

    @property
    def created_at(self) -> float:
        return self._created_at

    def to_dict(self):
        # return {f: getattr(self, f) for f in self.__struct_fields__}
        return structs.asdict(self)
