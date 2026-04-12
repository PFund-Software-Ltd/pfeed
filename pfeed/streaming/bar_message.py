from typing import Annotated

from msgspec import Meta

from pfeed.streaming.market_data_message import MarketDataMessage


class BarMessage(MarketDataMessage, frozen=True):
    start_ts: Annotated[float, Meta(gt=0, lt=10_000_000_000)]  # timestamp of the start of the bar
    end_ts: Annotated[float, Meta(gt=0, lt=10_000_000_000)]  # timestamp of the end of the bar
    open: Annotated[float, Meta(gt=0)]
    high: Annotated[float, Meta(gt=0)]
    low: Annotated[float, Meta(gt=0)]
    close: Annotated[float, Meta(gt=0)]
    volume: Annotated[float, Meta(ge=0)]
    is_incremental: bool  # if True, the bar update is incremental, otherwise it is a full bar update
    
    def __post_init__(self):
        if self.is_incremental and self.ts >= self.end_ts:
            raise ValueError(f"Timestamp {self.ts} is greater than end timestamp {self.end_ts} before the bar is closed")
        
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

    def is_bar(self) -> bool:
        return True
