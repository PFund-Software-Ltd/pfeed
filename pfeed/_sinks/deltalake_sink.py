# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
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
            partition_fields = [table.schema.field(n) for n in self._partition_columns]
            schema = pa.schema(list(self._schema) + partition_fields)
        else:
            schema = self._schema
        table = table.select(schema.names).cast(schema)
        self._io.write(
            data=table,
            table_path=path,
            partition_by=self._partition_columns,
        )
