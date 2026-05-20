from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow as pa

from pfeed._sinks.base_sink import BaseSink


# TODO: add buffer_path? where buffer_path = table_path in this case
class DeltaLakeSink(BaseSink):
    # TODO
    def open(self) -> None:
        pass

    def write(self, batch: pa.RecordBatch) -> None:
        pass

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
