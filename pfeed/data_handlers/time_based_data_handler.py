from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing import GenericFrame, tDATA_TOOL
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel

from abc import abstractmethod
from pathlib import Path
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
    
    @abstractmethod
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def _create_file_paths(self, data_model: TimeBasedDataModel | None=None) -> list[str]:
        data_model = data_model or self._data_model
        # data_layer!="curated" means that the data will be stored per date
        if self._data_layer != DataLayer.CURATED:
            return [
                self._data_path + '/' + str(data_model.create_storage_path(date)) + '/' + data_model.create_filename(date)
                for date in data_model.dates
            ]
        # data_layer="curated" means that the data will be stored in a single file
        else:
            # remove the date from the filename
            filename = data_model.create_filename(data_model.date).replace("_"+str(data_model.date), '')
            # only get the top 3 levels env/data_source/data_origin from the storage path
            storage_path = Path(*data_model.create_storage_path(data_model.date).parts[:3])
            return [self._data_path + '/' + str(storage_path) + '/' + filename]
        
    def write(self, df: GenericFrame, metadata: dict | None=None):
        from pfeed._etl.base import convert_to_pandas_df
        df: pd.DataFrame = convert_to_pandas_df(df)

        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
        df = self._validate_schema(df)

        data_model: TimeBasedDataModel = self._data_model
        if self._data_layer != DataLayer.CURATED:
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
        else:
            self._io.write(
                df, 
                file_path=self._create_file_paths(data_model=data_model)[0],
                metadata=metadata,
            )

    def read(self, data_tool: tDATA_TOOL='polars', delta_version: int | None=None) -> tuple[GenericFrame | None, dict[str, Any]]:
        from pfeed._etl.base import convert_to_user_df
        df, metadata = self._io.read(
            file_paths=self._create_file_paths(),
            data_tool=data_tool, 
            delta_version=delta_version,
        )
        data_model: TimeBasedDataModel = self._data_model
        # data is stored per date
        if self._data_layer != DataLayer.CURATED:
            missing_file_paths: list[str] = metadata['missing_file_paths']
            missing_dates = [fp.split('/')[-1].rsplit('_', 1)[-1].removesuffix(data_model.file_extension) for fp in missing_file_paths]
            missing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates]
        # data is stored in a single file (no date in the filename)
        else:
            if file_metadata := list(metadata['file_metadata'].values()):
                file_metadata = file_metadata[0]
                current_dates_in_storage = pd.date_range(file_metadata['start_date'], file_metadata['end_date']).date.tolist()
            else:
                current_dates_in_storage = []
            missing_dates = [date for date in data_model.dates if date not in current_dates_in_storage]
            if df is not None:
                # select the range of data_model.dates in the df
                # performance should not be an issue since storage.read_data() should be using polars already, call convert_to_user_df again to make sure
                polars_lf: pl.LazyFrame = convert_to_user_df(df, data_tool='polars')
                polars_lf = polars_lf.filter(pl.col('date').dt.date().is_in(data_model.dates))
                df = convert_to_user_df(polars_lf, data_tool=data_tool)

        metadata['missing_dates'] = missing_dates
        return df, metadata
