from __future__ import annotations
from typing import ClassVar, Any

from pydantic import BaseModel, ConfigDict, Field

from pfeed.enums import IOFormat, Compression


class IOConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    io_format: IOFormat | str = IOFormat.PARQUET
    compression: Compression | str = Compression.SNAPPY
    connect_options: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional options to pass to the IO class's connect(**connect_options) (if applicable)",
    )
    write_options: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional options to pass to the IO class's write(**write_options) (if applicable)",
    )
    read_options: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional options to pass to the IO class's read(**read_options) (if applicable)",
    )
