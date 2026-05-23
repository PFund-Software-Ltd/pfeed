# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed._io.deltalake_io import DeltaLakeIO
    from pfeed._io.table_io import TablePath

import pyarrow as pa

from pfeed._sinks.base_sink import BaseSink


class DeltaLakeSink(BaseSink):
    _io: DeltaLakeIO

    def write(self, data: dict[str, Any], path: TablePath) -> None:
        self._buffer.append(data)
        self._maybe_flush(path)

    def flush(self, path: TablePath) -> None:
        table = pa.Table.from_pylist(self._buffer)
        if self._partition_func is not None:
            table = self._partition_func(table)
        table = table.cast(self._schema)
        self._io.write(data=table, table_path=path)
