from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from psycopg import Connection as PostgresConnection
    from psycopg import AsyncConnection as AsyncPostgresConnection

import psycopg

from pfeed._io.database_io import DatabaseIO
from pfeed.enums import TimestampPrecision


class PostgreSQLIO(DatabaseIO):
    SUPPORTS_PARALLEL_WRITES: bool = True
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.MICROSECOND

    # TODO: handle async connection
    def _open_connection(self, uri: str) -> PostgresConnection | AsyncPostgresConnection:
        self._conn_uri = uri
        self._conn: PostgresConnection | AsyncPostgresConnection = psycopg.connect(uri, **self._io_options)
        return self._conn
    
    def _close_connection(self):
        self._conn.close()