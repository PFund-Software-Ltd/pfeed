from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfund.datas.resolution import Resolution
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_TOOL
    from pfeed.data_models.market_data_model import MarketDataModel

import pandas as pd

from pfeed.io.tabular_io import TabularIO
from pfeed.data_handlers.base_data_handler import BaseDataHandler


class MarketDataHandler(BaseDataHandler):
    def __init__(
        self, 
        data_model: MarketDataModel,
        data_path: str,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None = None,
        use_deltalake: bool = False,
    ):
        '''
        Args:
            data_path: data path that already consists of data layer and data domain
                but still has no information about the data model
        '''
        super().__init__(data_model, data_path)
        self._io = TabularIO(
            filesystem,
            compression=data_model.compression,
            storage_options=storage_options,
            use_deltalake=use_deltalake,
        )
    
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
        
    def write(self, data: tDataFrame):
        from pfeed.etl import convert_to_pandas_df
        # split data with a date range into chunks per date
        data: pd.DataFrame = convert_to_pandas_df(data)
        data = self._validate_schema(data)
        data_chunks_per_date = {} if data.empty else {date: group for date, group in data.groupby(data['ts'].dt.date)}
        for date in self._data_model.dates:
            data_model_copy: MarketDataModel = self._data_model.model_copy(deep=False)
            # NOTE: create placeholder data if date is not in data_chunks_per_date, 
            # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
            data_chunk = data_chunks_per_date.get(date, pd.DataFrame(columns=data.columns))
            # make date range (start_date, end_date) to (date, date), since storage is per date
            data_model_copy.update_start_date(date)
            data_model_copy.update_end_date(date)
            # if is_placeholder is true, it means there is no data on that date
            # without it, you can't know if the data is missing due to download failure or there is actually no data on that date
            data_model_copy.update_metadata('is_placeholder', 'true' if data_chunk.empty else 'false')
            self._io.write(
                data_chunk,
                file_path=self._create_file_path(data_model=data_model_copy),
                metadata=data_model_copy.metadata,
            )

    def read(self, data_tool: tDATA_TOOL='pandas', delta_version: int | None=None) -> tuple[tDataFrame | None, dict]:
        assert not self._data_model.is_date_range(), 'data model must only contain a single date'
        data, metadata = self._io.read(
            file_path=self._create_file_path(self._data_model),
            data_tool=data_tool, 
            delta_version=delta_version,
        )
        return data, metadata
