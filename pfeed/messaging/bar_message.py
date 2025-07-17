from typing import Annotated
from msgspec import Meta, Struct, field, ValidationError


class BarMessage(
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
    # if timestamp's unit is second, it should have 10 digits in the next 200 years
    ts: Annotated[float, Meta(gt=0, lt=10_000_000_000)]
    open: Annotated[float, Meta(gt=0)]
    high: Annotated[float, Meta(gt=0)]
    low: Annotated[float, Meta(gt=0)]
    close: Annotated[float, Meta(gt=0)]
    volume: Annotated[float, Meta(ge=0)]
    extra_data: dict = field(default_factory=dict)
    
    def __post_init__(self):
        # Validate high is highest
        if not (self.high >= self.open and self.high >= self.low and self.high >= self.close):
            raise ValueError(f"High ({self.high}) must be >= open, low, and close")
            
        # Validate low is lowest
        if not (self.low <= self.open and self.low <= self.high and self.low <= self.close):
            raise ValueError(f"Low ({self.low}) must be <= open, high, and close")
            
        # Validate open/close within high/low range
        if not (self.low <= self.open <= self.high):
            raise ValueError(f"Open ({self.open}) must be between low and high")
            
        if not (self.low <= self.close <= self.high):
            raise ValueError(f"Close ({self.close}) must be between low and high")
            
    
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}


