from types import NoneType
from typing import ClassVar, get_args

import polars as pl

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.enums import IOFormat


class BaseSQLDataModel(BaseDataModel):
    table_name: ClassVar[str]
    table_sql: ClassVar[str] = ""
    insert_sql: ClassVar[dict[IOFormat, str]] = {}

    @classmethod
    def column_names(cls) -> tuple[str, ...]:
        routing_fields = set(BaseDataModel.model_fields)
        return tuple(
            field_name
            for field_name in cls.model_fields
            if field_name not in routing_fields
        )

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {
            column_name: NoneType in get_args(cls.model_fields[column_name].annotation)
            for column_name in cls.column_names()
        }

    def to_frame(self) -> pl.DataFrame:
        columns = set(self.column_names())
        record = self.model_dump(mode="json", include=columns)
        return pl.DataFrame([record])
