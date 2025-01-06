import datetime

import pandera as pa
from pandera.typing import Series


class MarketDataSchema(pa.DataFrameModel):
    ts: Series[datetime.datetime]
    resolution: Series[str] = pa.Field(isin=[
        '1q_L1', '1q_L2', '1q_L3', '1t',
        '1s', '1m', '1h', '1d',
        '1w', '1M', '1y',
    ])
    product: Series[str]
    symbol: Series[str] | None
    
    @pa.check('ts', error='ts is not monotonic increasing')
    def validate_ts(cls, ts: Series[datetime.datetime]) -> bool:
        return ts.is_monotonic_increasing
