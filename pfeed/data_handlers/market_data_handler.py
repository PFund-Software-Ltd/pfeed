from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from pfund.datas.resolution import Resolution

from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class MarketDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, data: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import MarketDataSchema, TickDataSchema, BarDataSchema
        resolution: Resolution = self._data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        elif resolution.is_tick():
            schema = TickDataSchema
        elif resolution.is_bar():
            schema = BarDataSchema
        else:
            schema = MarketDataSchema
        return schema.validate(data)
 