from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing import GenericFrame, StorageMetadata
    from pfeed.data_models.time_based_data_model import (
        TimeBasedDataModel, 
        TimeBasedMetadata, 
        TimeBasedDeltaMetadata
    )

import datetime
import json

import pandas as pd
import polars as pl

from pfeed.enums import DataLayer
from pfeed._io.tabular_io import TabularIO
from pfeed.data_handlers.base_data_handler import BaseDataHandler
from pfeed.storages.delta_lake_storage_mixin import DeltaLakeStorageMixin


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
        super().__init__(
            data_model=data_model, 
            data_layer=data_layer, 
            data_path=data_path, 
            use_deltalake=use_deltalake,
        )
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
        if not self._use_deltalake:
            file_paths = [
                '/'.join([self._data_path, str(data_model.create_storage_path(date)), data_model.create_filename(date)])
                for date in data_model.dates
            ]
        else:
            file_paths = ['/'.join([self._data_path, str(data_model.create_storage_path(data_model.dates[0]))])]
        return file_paths
        
    def write(self, df: GenericFrame, metadata: dict | None=None):
        from pfeed._etl.base import convert_to_pandas_df

        Metadata: TimeBasedMetadata | dict[str, Any]
        DeltaTableMetadata: TimeBasedDeltaMetadata | dict[str, Any]
        metadata: Metadata = {**self._data_model.to_metadata(), **(metadata or {})}

        df: pd.DataFrame = convert_to_pandas_df(df)
        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
        df = self._validate_schema(df)

        data_model: TimeBasedDataModel = self._data_model
        # split data with a date range into chunks per date
        data_chunks_per_date = {} if df.empty else {date: group for date, group in df.groupby(df['date'].dt.date)}
        if not self._use_deltalake:
            for date in data_model.dates:
                data_model_copy = data_model.model_copy(deep=False)
                # NOTE: create placeholder data if date is not in data_chunks_per_date, 
                # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
                df_chunk = data_chunks_per_date.get(date, pd.DataFrame(columns=df.columns))
                # make date range (start_date, end_date) to (date, date), since storage is per date
                data_model_copy.update_start_date(date)
                data_model_copy.update_end_date(date)
                file_path = self._create_file_paths(data_model=data_model_copy)[0]
                self._io.write(file_path, df_chunk, metadata)
        else:
            # Preprocess table to add year, month, day columns for partitioning
            delta_partition_by = ['year', 'month', 'day']
            df = df.assign(
                year=df["date"].dt.year,
                month=df["date"].dt.month,
                day=df["date"].dt.day,
            )
            file_path = self._create_file_paths(data_model=data_model)[0]
            metadata_file_path = file_path + '/' + DeltaLakeStorageMixin.metadata_filename
            if self._io._exists(metadata_file_path):
                existing_metadata: dict[str, Any] = self._io._read_pyarrow_table_metadata([metadata_file_path])
                file_metadata: DeltaTableMetadata = existing_metadata.get(metadata_file_path, {})
                existing_dates = file_metadata.get('dates', [])
            else:
                existing_dates = []
            dates_in_data = pd.date_range(metadata.pop('start_date'), metadata.pop('end_date')).date.tolist()
            total_dates = list(set(dates_in_data + existing_dates))
            metadata['dates'] = total_dates
            self._io.write(file_path, df, metadata, delta_partition_by=delta_partition_by)

    def read(self, delta_version: int | None=None) -> tuple[pl.LazyFrame | None, StorageMetadata]:
        lf, metadata = self._io.read(file_paths=self._create_file_paths(), delta_version=delta_version)
        data_model: TimeBasedDataModel = self._data_model
        missing_file_paths: list[str] = metadata['missing_file_paths']
        if not self._use_deltalake:
            missing_dates = [fp.split('/')[-1].rsplit('_', 1)[-1].removesuffix(data_model.file_extension) for fp in missing_file_paths]
            missing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates]
            metadata['missing_dates'] = missing_dates
        else:
            file_metadata = metadata['file_metadata']
            deltalake_metadata = list(file_metadata.values())[0]
            existing_dates = deltalake_metadata.get('dates', [])
            existing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in existing_dates]
            metadata['missing_dates'] = [date for date in data_model.dates if date not in existing_dates]
        return lf, metadata
