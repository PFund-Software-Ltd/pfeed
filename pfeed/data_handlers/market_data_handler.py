from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pandas as pd
    from pfeed.typing import GenericFrame, tDATA_TOOL
    from pfeed.data_models.market_data_model import MarketDataModel

from pfund.datas.resolution import Resolution
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler
from pfeed.enums import DataLayer


class MarketDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import MarketDataSchema, TickDataSchema, BarDataSchema
        data_model: MarketDataModel = self._data_model
        resolution: Resolution = data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        elif resolution.is_tick():
            schema = TickDataSchema
        elif resolution.is_bar():
            schema = BarDataSchema
        else:
            schema = MarketDataSchema
        return schema.validate(df)

    def write(self, df: GenericFrame):
        data_model: MarketDataModel = self._data_model
        metadata = {
            'start_date': data_model.start_date, 
            'end_date': data_model.end_date,
            'resolution': repr(data_model.resolution),
            'product': data_model.product.name,
            'product_type': data_model.product.type,
            'data_source': data_model.data_source.name,
            'data_origin': data_model.data_origin,
        }
        return super().write(df, metadata=metadata)

    def read(self, data_tool: tDATA_TOOL='polars', delta_version: int | None=None) -> tuple[GenericFrame | None, dict[str, Any]]:
        df, metadata = super().read(data_tool=data_tool, delta_version=delta_version)
        # check if df in curated data layer has the same resolution as the data model, if not, abandon df
        if df is not None and self._data_layer == DataLayer.CURATED:
            data_model: MarketDataModel = self._data_model
            file_metadata = list(metadata['file_metadata'].values())[0]
            df_resolution = Resolution(file_metadata['resolution'])
            if df_resolution != data_model.resolution:
                df = None
                metadata['missing_dates'] = data_model.dates
        return df, metadata
            
                