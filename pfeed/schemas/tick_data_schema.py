import pandera as pa
from pandera.typing import Series

from pfeed.schemas import MarketDataSchema


class TickDataSchema(MarketDataSchema):
    price: Series[float] = pa.Field(gt=0)
    side: Series[int] = pa.Field(isin=[1, -1])
    volume: Series[float] = pa.Field(gt=0)
    
    @pa.check('side')
    def validate_side_has_both_buy_and_sell(cls, side: Series[int]) -> bool:
        return side.nunique() == 2
