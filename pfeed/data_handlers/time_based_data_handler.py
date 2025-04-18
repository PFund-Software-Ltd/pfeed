from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing import GenericFrame, tDATA_TOOL
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel

import datetime

import pandas as pd
import polars as pl

from pfeed.enums import DataLayer
from pfeed._io.tabular_io import TabularIO
from pfeed.data_handlers.base_data_handler import BaseDataHandler


class TimeBasedDataHandler(BaseDataHandler):
    def __init__(
        self, 
        data_model: TimeBasedDataModel,
        data_layer: DataLayer,
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
        super().__init__(data_model, data_layer, data_path)
        self._io = TabularIO(
            filesystem=filesystem,
            compression=data_model.compression,
            storage_options=storage_options,
            use_deltalake=use_deltalake,
        )
    
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def _create_file_paths(self, data_model: TimeBasedDataModel | None=None) -> list[str]:
        data_model = data_model or self._data_model
        return [
            self._data_path + '/' + str(data_model.create_storage_path(date)) + '/' + data_model.create_filename(date)
            for date in data_model.dates
        ]
        
    def write(self, df: GenericFrame, metadata: dict | None=None):
        from pfeed._etl.base import convert_to_pandas_df

        metadata = {**self._data_model.to_metadata(), **(metadata or {})}

        df: pd.DataFrame = convert_to_pandas_df(df)
        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
        df = self._validate_schema(df)

        data_model: TimeBasedDataModel = self._data_model
        # split data with a date range into chunks per date
        data_chunks_per_date = {} if df.empty else {date: group for date, group in df.groupby(df['date'].dt.date)}
        for date in data_model.dates:
            data_model_copy = data_model.model_copy(deep=False)
            # NOTE: create placeholder data if date is not in data_chunks_per_date, 
            # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
            df_chunk = data_chunks_per_date.get(date, pd.DataFrame(columns=df.columns))
            # make date range (start_date, end_date) to (date, date), since storage is per date
            data_model_copy.update_start_date(date)
            data_model_copy.update_end_date(date)
            self._io.write(
                df_chunk, 
                file_path=self._create_file_paths(data_model=data_model_copy)[0],
                metadata=metadata,
            )

    def read(self, data_tool: tDATA_TOOL='polars', delta_version: int | None=None) -> tuple[GenericFrame | None, dict[str, Any]]:
        df, metadata = self._io.read(
            file_paths=self._create_file_paths(),
            data_tool=data_tool, 
            delta_version=delta_version,
        )
        data_model: TimeBasedDataModel = self._data_model
        missing_file_paths: list[str] = metadata['missing_file_paths']
        missing_dates = [fp.split('/')[-1].rsplit('_', 1)[-1].removesuffix(data_model.file_extension) for fp in missing_file_paths]
        missing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates]
        metadata['missing_dates'] = missing_dates
        return df, metadata
