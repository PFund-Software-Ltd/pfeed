import json
from types import NoneType
from typing import Annotated, Any, ClassVar, Literal, cast, get_args, get_origin
from uuid import UUID

import polars as pl
from pydantic import model_validator

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.enums import IOFormat

_POLARS_DTYPES: dict[Any, pl.DataType] = {
    bool: pl.Boolean(),
    int: pl.Int64(),
    float: pl.Float64(),
    str: pl.String(),
    UUID: pl.String(),
}


def _unwrap_annotation(annotation: Any) -> Any:
    """Unwrap nullable and Annotated layers without unwrapping containers."""
    while True:
        origin = get_origin(annotation)
        if origin is Annotated:
            annotation = get_args(annotation)[0]
            continue
        args = tuple(arg for arg in get_args(annotation) if arg is not NoneType)
        if len(args) == 1 and origin is not list:
            annotation = args[0]
            continue
        return annotation


def _is_list_annotation(annotation: Any) -> bool:
    return get_origin(_unwrap_annotation(annotation)) is list


def _resolve_dtype(annotation: Any) -> pl.DataType:
    """Reduce a pydantic annotation to the polars dtype its column is stored as.

    Unwraps Optional/Annotated layers, stores lists as JSON strings, and treats
    Literal as its string form.
    """
    seen = annotation
    while True:
        if _is_list_annotation(annotation):
            return pl.String()
        if annotation in _POLARS_DTYPES:
            return _POLARS_DTYPES[annotation]
        if get_origin(annotation) is Literal:
            return pl.String()
        args = tuple(arg for arg in get_args(annotation) if arg is not NoneType)
        if not args:
            raise TypeError(f"No polars dtype maps to annotation {seen!r}")
        annotation = args[0]


class BaseTableDataModel(BaseDataModel):
    table_name: ClassVar[str]
    table_sql: ClassVar[str] = ""
    index_sql: ClassVar[dict[IOFormat, tuple[str, ...]]] = {}
    insert_sql: ClassVar[dict[IOFormat, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def decode_json_list_columns(cls, data: Any) -> Any:
        """Decode SQL JSON strings for fields represented as Python lists."""
        if not isinstance(data, dict):
            return data

        decoded = cast(dict[str, Any], data.copy())
        for column_name in cls.column_names():
            annotation = cls.model_fields[column_name].annotation
            value = decoded.get(column_name)
            if not _is_list_annotation(annotation) or not isinstance(value, str):
                continue
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{column_name} must be a JSON array") from exc
            if not isinstance(value, list):
                raise ValueError(f"{column_name} must be a JSON array")
            decoded[column_name] = value
        return decoded

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

    @classmethod
    def polars_schema(cls) -> dict[str, pl.DataType]:
        """The dtype every column is written as, regardless of the row's values.

        Without this, a column that happens to be None on the first write is
        inferred as pl.Null and frozen into the database's arrow schema, which
        then rejects every later row that actually has a value.
        """
        return {
            column_name: _resolve_dtype(cls.model_fields[column_name].annotation)
            for column_name in cls.column_names()
        }

    def to_frame(self) -> pl.DataFrame:
        columns = set(self.column_names())
        record = self.model_dump(mode="json", include=columns)
        for column_name in columns:
            annotation = type(self).model_fields[column_name].annotation
            value = record.get(column_name)
            if _is_list_annotation(annotation) and value is not None:
                record[column_name] = json.dumps(value)
        return pl.DataFrame([record])
