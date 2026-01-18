from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from psycopg import Connection as PostgresConnection
    from psycopg import AsyncConnection as AsyncPostgresConnection

import os

import psycopg

from pfeed._io.database_io import DatabaseIO
from pfeed.enums import TimestampPrecision


class PostgreSQLIO(DatabaseIO):
    SCHEME: str = "postgresql"
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.MICROSECOND

    def __init__(self, storage_options: dict | None = None, io_options: dict | None = None):
        super().__init__(storage_options=storage_options, io_options=io_options)
        self.user = os.getenv("POSTGRES_USER", "pfunder")
        self.password = os.getenv("POSTGRES_PASSWORD", "password")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
    
    # e.g. postgresql://pfunder:password@localhost:5432/my_db
    def _create_uri(self, db_name: str) -> str:
        return f"{self.SCHEME}://{self.user}:{self.password}@{self.host}:{self.port}/{db_name}"
    
    # TODO: handle async connection
    def _open_connection(self, uri: str) -> PostgresConnection | AsyncPostgresConnection:
        self._conn_uri = uri
        self._conn: PostgresConnection | AsyncPostgresConnection = psycopg.connect(uri, **self._io_options)
        return self._conn
    
    def _close_connection(self):
        self._conn.close()