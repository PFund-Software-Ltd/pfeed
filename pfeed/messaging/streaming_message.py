from typing import Annotated, Optional

import time

from msgspec import Meta, Struct, field, ValidationError, structs


class StreamingMessage(
    Struct, 
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
    array_like=True,  # NOTE: setting array_like=True for 1.5-2x speedup
    gc=True,  # OPTIMIZE: consider setting gc=False for performance boost (but then you can't use lists, dicts etc.)
):
    product: str  # product basis, e.g. BTC_USDT_PERP
    symbol: str
    specs: dict  # product specifications, e.g. for options, specs are strike_price, expiration, etc.
    resolution: str
    # if timestamp's unit is second, it should have only 10 digits in the next 200 years
    # NOTE: msg_ts vs ts, e.g. for candlesticks, "msg_ts" is the timestamp of the message sent by the exchange, "ts" is the timestamp of the candlestick
    ts: Annotated[float, Meta(gt=0, lt=10_000_000_000)]  # timestamp of the data
    msg_ts: Optional[Annotated[float, Meta(gt=0, lt=10_000_000_000)]] = None  # timestamp of the message sent
    _created_at: float = field(default_factory=time.time)  # timestamp of this object creation
    extra_data: dict = field(default_factory=dict)
    # NOTE: custom fields that get flattened during write (not nested under "custom_data")
    # if this default behaviour is not desired, users can just create their own class that inherits from this class
    custom_data: dict = field(default_factory=dict)
    
    @property
    def created_at(self) -> float:
        return self._created_at

    def to_dict(self):
        # return {f: getattr(self, f) for f in self.__struct_fields__}
        return structs.asdict(self)