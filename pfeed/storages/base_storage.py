# pyright: reportUnknownMemberType=false, reportAttributeAccessIssue=false, reportUnknownArgumentType=false
from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Any, Self, cast

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame
    from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseDataMetadata, SourcePath
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed._io.base_io import BaseIO
    from pfeed.streaming.streaming_message import StreamingMessage
    from pfeed.storages.storage_config import StorageConfig
    from pfeed._io.io_config import IOConfig

import datetime
from pprint import pformat

import polars as pl

from pydantic import Field, BaseModel, ConfigDict

from pfeed.enums import DataLayer, IOFormat
from pfeed.utils.file_path import FilePath
from pfeed.data_handlers.base_data_handler import BaseDataMetadata


class StorageMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data: dict[SourcePath, BaseDataMetadata]
    missing_dates_in_storage: list[datetime.date] | None = Field(
        default=None,
        description="Dates within the requested data period that have data at the source but do not exist in local storage."
    )
    missing_source_paths: list[SourcePath]
    # missing_dates_at_source: list[datetime.date] = Field(
    #     description="Dates within the requested data period where no data exists at the source (e.g. non-trading days, holidays)."
    # )

    def __str__(self) -> str:
        from textwrap import indent
        lines: list[str] = [f"{type(self).__name__}("]
        for key, value in self.model_dump().items():
            if isinstance(value, dict):
                lines.append(f"  {key}={{")
                items = list(value.items())
                for i, (k, v) in enumerate(items):
                    lines.append(f"    [{i}] '{k}':")
                    lines.append(indent(pformat(v, sort_dicts=False), "        "))
                    if i < len(items) - 1:
                        lines.append("")
                lines.append("  }")
            elif isinstance(value, list) and value:
                lines.append(f"  {key}=[")
                lines.extend(f"    {item}," for item in value)
                lines.append("  ]")
            else:
                lines.append(f"  {key}={pformat(value, sort_dicts=False)}")
        lines.append(")")
        return "\n".join(lines)

    __repr__ = __str__


class BaseStorage:
    SUPPORTED_IO_FORMATS: ClassVar[list[IOFormat]] = []

    def __init__(
        self,
        data_path: FilePath | DatabaseURI,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
    ):
        '''
        Args:
            data_layer: Data layer to store the data.
            data_domain: Data domain of the data, used for grouping data inside a data layer.
            storage_options: Storage options
        '''
        self.data_path = data_path
        self.data_layer = DataLayer[str(data_layer).upper()]
        self.data_domain = data_domain.upper()
        self.storage_options: dict[str, Any] = storage_options or {}
        self._data_model: BaseDataModel | None = None
        self._data_handler: BaseDataHandler | None = None
        self._io: BaseIO | None = None
        self._is_data_handler_stale: bool = False

    def _get_io_kwargs(self) -> dict[str, Any]:
        return {}

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        parts = []
        if self._io:
            parts.append(f"io={self._io.__class__.__name__}")
        if self._data_model:
            parts.append(f"data_model={self._data_model}")
        parts.extend(
            [
                f"data_path={self.data_path}",
                f"data_layer={self.data_layer}",
                f"data_domain={self.data_domain}",
            ]
        )
        if self.storage_options:
            parts.append(f"storage_options={self.storage_options}")
        return f"{self.__class__.__name__} (" + " | ".join(parts) + ")"

    def __repr__(self) -> str:
        data = {
            "data_path": self.data_path,
            "data_layer": self.data_layer,
            "data_domain": self.data_domain,
            "storage_options": self.storage_options,
            "data_model": str(self._data_model),
            "io": self._io.name if self._io else None,
            "data_handler": self._data_handler,
        }
        return f"{self.name}(\n{pformat(data, sort_dicts=False)}\n)"

    @property
    def data_model(self) -> BaseDataModel:
        if not self._data_model:
            raise AttributeError(f"No data model has been set for storage: {self.name}")
        return self._data_model

    @property
    def data_handler(self) -> BaseDataHandler:
        # re-initialize data handler if data model or io has changed
        if self._is_data_handler_stale:
            self._initialize_data_handler()
            self._is_data_handler_stale = False
        if not self._data_handler:
            raise AttributeError(
                f"No data handler has been set for storage: {self.name}, please call with_data_model() first"
            )
        return self._data_handler

    @property
    def io(self) -> BaseIO:
        if not self._io:
            raise AttributeError(f"No IO has been set for storage: {self.name}")
        return self._io

    @classmethod
    def from_storage_config(cls, storage_config: StorageConfig) -> Self:
        return cls(**storage_config.model_dump())

    def with_data_model(self, data_model: BaseDataModel) -> BaseStorage:
        self._data_model = data_model
        self._is_data_handler_stale = True
        return self

    def with_io(self, io_config: IOConfig) -> BaseStorage:
        IO = cast(type[BaseIO], io_config.io_format.io_class)
        self._io = IO(
            storage_options=self.storage_options,
            **io_config.model_dump(),
            **self._get_io_kwargs(),  # io kwargs specific to the storage, e.g filesystem from a file-based storage
        )
        self._is_data_handler_stale = True
        return self

    def _initialize_data_handler(self) -> None:
        DataHandler: type[BaseDataHandler] = self.data_model.data_handler_class
        self._data_handler = DataHandler(
            data_path=self.data_path,
            data_layer=self.data_layer,
            data_domain=self.data_domain,
            data_model=self.data_model,
            io=self.io,
        )

    def write_data(self, data: IntoFrame | StreamingMessage, streaming: bool = False):
        self.data_handler.write(data=data, streaming=streaming)

    def read_data(self) -> pl.LazyFrame | None:
        """Read data from storage."""
        return self.data_handler.read()

    def read_metadata(self) -> StorageMetadata:
        from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler
        if isinstance(self.data_handler, TimeBasedDataHandler):
            missing_dates_in_storage = self.data_handler.find_missing_dates_in_storage()
        else:
            missing_dates_in_storage = None
        return StorageMetadata(
            data=self.data_handler.read_metadata(),
            missing_source_paths=cast("list[SourcePath]", self.data_handler.find_missing_source_paths()),
            missing_dates_in_storage=missing_dates_in_storage
        )
