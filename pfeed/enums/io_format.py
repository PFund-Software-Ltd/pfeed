from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed._io.base_io import BaseIO

from enum import StrEnum


def _is_parquet(data: bytes) -> bool:
    """Detect if data is in Parquet format by checking magic bytes."""
    return len(data) >= 4 and data[:4] == b'PAR1'


def _is_likely_csv(data: bytes, sample_size: int = 4096) -> bool:
    """Detect if data is likely in CSV format using heuristics."""
    import csv
    try:
        # Try to decode a sample of the data
        sample = data[:sample_size].decode('utf-8', errors='ignore')
        lines = sample.splitlines()
        if len(lines) < 2:
            return False

        # Use csv.Sniffer to detect if it's a CSV
        sniffer = csv.Sniffer()
        sniffer.sniff(sample)  # Will raise exception if not CSV-like
        sniffer.has_header(sample)

        # If we've gotten this far without an exception, it's likely a CSV
        return True
    except Exception:
        # If any exception occurred, it's probably not a CSV
        return False


class IOFormat(StrEnum):
    # CSV = 'csv'
    PARQUET = 'parquet'
    # REVIEW: see if need to separate this into table format?
    DELTALAKE = 'deltalake'
    DUCKDB = 'duckdb'
    LANCEDB = 'lancedb'

    @property
    def io_class(self) -> type[BaseIO]:
        if self == IOFormat.PARQUET:
            from pfeed._io.parquet_io import ParquetIO
            return ParquetIO
        elif self == IOFormat.DELTALAKE:
            from pfeed._io.deltalake_io import DeltaLakeIO
            return DeltaLakeIO
        elif self == IOFormat.DUCKDB:
            # this import will throw ImportError if duckdb is not installed
            from pfeed._io.duckdb_io import DuckDBIO
            return DuckDBIO
        elif self == IOFormat.LANCEDB:
            # this import will throw ImportError if lancedb is not installed
            from pfeed._io.lancedb_io import LanceDBIO
            return LanceDBIO
        else:
            raise ValueError(f'{self=} is not supported')

    @staticmethod
    def detect(data: bytes) -> IOFormat | None:
        """Detect storage format from data bytes.

        Returns the detected IOFormat or None if format cannot be determined.
        Only works for single-file formats (PARQUET, CSV).
        e.g. LANCEDB, DELTALAKE and DUCKDB are directory-based and cannot be detected from bytes.
        """
        if _is_parquet(data):
            return IOFormat.PARQUET
        elif _is_likely_csv(data):
            return IOFormat.CSV
        return None
