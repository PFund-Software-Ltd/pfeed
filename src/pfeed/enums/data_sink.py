from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed._sinks.base_sink import BaseSink
    from pfeed.enums import IOFormat

from enum import StrEnum


class DataSink(StrEnum):
    DELTALAKE = "DELTALAKE"

    @property
    def sink_class(self) -> type[BaseSink]:
        if self == DataSink.DELTALAKE:
            from pfeed._sinks.deltalake_sink import DeltaLakeSink

            return DeltaLakeSink
        else:
            raise ValueError(f"{self=} is not supported")

    @property
    def io_format(self) -> IOFormat:
        from pfeed.enums import IOFormat

        if self == DataSink.DELTALAKE:
            return IOFormat.DELTALAKE
        else:
            raise ValueError(f"{self=} is not supported")
