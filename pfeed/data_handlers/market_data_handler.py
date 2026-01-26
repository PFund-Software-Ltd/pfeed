from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from pfeed.data_models.market_data_model import MarketDataModel
    
import datetime

from pfund.datas.resolution import Resolution
from pfeed.utils.file_path import FilePath
from pfeed._io.table_io import TablePath
from pfeed._io.database_io import DBPath
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class MarketDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import MarketDataSchema, TickDataSchema, BarDataSchema
        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
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
    
    def _create_file_path(self, date: datetime.date) -> FilePath:
        data_model: MarketDataModel = self._data_model
        product = data_model.product
        filename = '_'.join([product.symbol, str(date)])
        file_extension = self._get_file_extension()
        year, month, day = str(date).split('-')
        table_path = self._create_table_path()
        return FilePath(
            table_path
            / f'year={year}'
            / f'month={month}'
            / f'day={day}'
            / (filename + file_extension)
        )
    
    def _create_table_path(self) -> TablePath:
        data_model: MarketDataModel = self._data_model
        product = data_model.product
        return TablePath(
            self._data_path
            / f"env={data_model.env}"
            / f"data_layer={self._data_layer}"
            / f'data_domain={self._data_domain}'
            / f'data_source={data_model.data_source.name}'
            / f'data_origin={data_model.data_origin}'
            / f'asset_type={str(product.asset_type)}'
            / f'resolution={repr(data_model.resolution)}'
        )

    def _create_db_path(self):
        data_model = self._data_model
        product = data_model.product
        db_name = data_model.env
        schema_name = "_".join([
            f"{self._data_layer}",
            f"{self._data_domain}",
            f"{data_model.data_source.name}",
            f"{data_model.data_origin}",
        ]).lower()
        table_name = '_'.join([
            str(product.asset_type),
            str(data_model.resolution),
        ]).lower()
        # NOTE: special case "duckdb" where its file io and database io at the same time
        if self._is_file_io(strict=False):
            db_uri = self._data_path / (db_name + self._io.FILE_EXTENSION)
            db_uri = str(db_uri)
        # NOTE: special case "lancedb" where its table io and database io at the same time
        elif self._is_table_io(strict=False):
            table_path = self._create_table_path()
            db_uri = str(table_path)
        else:
            db_uri = self._data_path + '/' + db_name
        return DBPath(db_uri=db_uri, db_name=db_name, schema_name=schema_name, table_name=table_name)