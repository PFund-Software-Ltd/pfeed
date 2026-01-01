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


class FileFormat(StrEnum):
    PARQUET = 'parquet'
    CSV = 'csv'
    # REVIEW: see if need to separate this into table format?
    DELTALAKE = 'deltalake'
    DUCKDB = 'duckdb'

    @property
    def io_class(self) -> type[BaseIO]:
        from pfeed._io.parquet_io import ParquetIO
        from pfeed._io.deltalake_io import DeltaLakeIO
        return {
            FileFormat.PARQUET: ParquetIO,
            FileFormat.DELTALAKE: DeltaLakeIO,
        }[self]

    @staticmethod
    def detect(data: bytes) -> 'FileFormat | None':
        """Detect file format from data bytes.

        Returns the detected FileFormat or None if format cannot be determined.
        Note: Only works for single-file formats (PARQUET, CSV).
        DELTALAKE and DUCKDB are directory-based and cannot be detected from bytes.
        """
        if _is_parquet(data):
            return FileFormat.PARQUET
        elif _is_likely_csv(data):
            return FileFormat.CSV
        return None