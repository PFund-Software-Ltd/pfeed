from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_TOOL
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel

from abc import abstractmethod

import pandas as pd

from pfeed._io.tabular_io import TabularIO
from pfeed.data_handlers.base_data_handler import BaseDataHandler


class TimeBasedDataHandler(BaseDataHandler):
    def __init__(
        self, 
        data_model: TimeBasedDataModel,
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
    
    @abstractmethod
    def _validate_schema(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    def _create_file_paths(self, data_model: TimeBasedDataModel | None=None) -> str | list[str]:
        data_model = data_model or self._data_model
        if data_model.is_date_range():
            return [self._data_path + '/' + str(data_model._create_storage_path(date)) + '/' + data_model._create_filename(date) for date in data_model.dates]
        else:
            return super()._create_file_paths(data_model)
        
    def write(self, data: tDataFrame):
        from pfeed._etl.base import convert_to_pandas_df
        data: pd.DataFrame = convert_to_pandas_df(data)
        data = self._validate_schema(data)
        # split data with a date range into chunks per date
        data_chunks_per_date = {} if data.empty else {date: group for date, group in data.groupby(data['date'].dt.date)}
        for date in self._data_model.dates:
            data_model_copy: TimeBasedDataModel = self._data_model.model_copy(deep=False)
            # NOTE: create placeholder data if date is not in data_chunks_per_date, 
            # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
            data_chunk = data_chunks_per_date.get(date, pd.DataFrame(columns=data.columns))
            # make date range (start_date, end_date) to (date, date), since storage is per date
            data_model_copy.update_start_date(date)
            data_model_copy.update_end_date(date)
            self._io.write(
                data_chunk,
                file_path=self._create_file_paths(data_model=data_model_copy),
            )

    def read(self, data_tool: tDATA_TOOL='polars', delta_version: int | None=None) -> tuple[tDataFrame | None, dict[str, Any]]:
        df, metadata = self._io.read(
            file_paths=self._create_file_paths(),
            data_tool=data_tool, 
            delta_version=delta_version,
        )
        return df, metadata
