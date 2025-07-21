from typing import Annotated, Optional

from msgspec import Meta, Struct, field, ValidationError


class StreamingMessage(
    Struct, 
    kw_only=True, 
    frozen=True, 
    omit_defaults=True,
    forbid_unknown_fields=True, 
    array_like=True,  # NOTE: setting array_like=True for 1.5-2x speedup
    gc=True,  # OPTIMIZE: consider setting gc=False for performance boost (but then you can't use lists, dicts etc.)
):
    trading_venue: str
    exchange: str
    product: str
    symbol: str
    resolution: str
    extra_data: dict = field(default_factory=dict)
    # if timestamp's unit is second, it should have only 10 digits in the next 200 years
    msg_ts: Optional[Annotated[float, Meta(gt=0, lt=10_000_000_000)]] = None  # timestamp of the message sent
    ts: Annotated[float, Meta(gt=0, lt=10_000_000_000)]  # timestamp of the data

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}