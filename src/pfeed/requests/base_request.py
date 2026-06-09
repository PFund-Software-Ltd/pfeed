from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pfeed._io.io_config import IOConfig
from pfeed._sinks.sink_config import SinkConfig
from pfeed.enums import DataLayer, DataSource, ExtractType
from pfeed.storages.storage_config import StorageConfig


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data_source: DataSource | str
    data_origin: str = ""
    extract_type: ExtractType
    storage_config: StorageConfig | None = None
    io_config: IOConfig | None = None
    sink_config: SinkConfig | None = None
    clean_data: bool = Field(
        default=True,
        description="""
            Whether to clean raw data using the default transformations (normalize, standardize columns, resample, etc.).
            For download/stream: when storage_config is provided, this parameter is ignored — cleaning is determined by storage_config.data_layer instead (resolved at load() time via finalize_load_config).
            For retrieve: when storage_config_for_retrieval.data_layer is not RAW, this parameter is forced to False — already-cleaned source data is never re-cleaned (resolved at request construction via model_post_init).
            If True, raw data will be cleaned.
            If False, raw data will be returned as is.
        """,
    )

    def __hash__(self) -> int:
        return hash(id(self))

    def __eq__(self, other: object) -> bool:
        return self is other

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def is_streaming(self) -> bool:
        return False

    def is_replaying(self) -> bool:
        return False

    @field_validator("data_source", mode="before")
    @classmethod
    def _validate_data_source(cls, value: DataSource | str) -> DataSource:
        if isinstance(value, str):
            return DataSource[value]
        return value

    def model_post_init(self, __context: Any) -> None:
        if not self.data_origin:
            self.data_origin = str(self.data_source)

    def finalize_load_config(
        self,
        storage_config: StorageConfig | None,
        io_config: IOConfig | None,
        sink_config: SinkConfig | None,
    ) -> None:
        """Finalize the storage/io config actually used by this request.

        because in pipeline mode storage_config and io_config are unknown
        at request construction time and only become final when .load() is invoked.
        """
        self.storage_config = storage_config
        self.io_config = io_config
        self.sink_config = sink_config
        if storage_config:
            is_raw_data = storage_config.data_layer == DataLayer.RAW
            # clean_data is already determined during request creation, no need to finalize for ExtractType.retrieve
            if self.extract_type != ExtractType.retrieve:
                self.clean_data = not is_raw_data
            if self.is_streaming() and is_raw_data:
                raise RuntimeError("Writing raw data in streaming is not supported")
