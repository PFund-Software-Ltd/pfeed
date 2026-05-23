from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed._sinks.base_sink import BaseSink
    from pfeed._io.base_io import BaseIO

from enum import StrEnum


class DataSink(StrEnum):
    DELTALAKE = "DELTALAKE"

    @property
    def sink_class(self) -> type[BaseSink]:
        if self == DataSink.DELTALAKE:
            from pfeed._sinks.deltalake_sink import DeltaLakeSink
            return DeltaLakeSink
        else:
            raise ValueError(f'{self=} is not supported')

    @property
    def io_class(self) -> type[BaseIO]:
        if self == DataSink.DELTALAKE:
            from pfeed._io.deltalake_io import DeltaLakeIO
            return DeltaLakeIO
        else:
            raise ValueError(f'{self=} is not supported')