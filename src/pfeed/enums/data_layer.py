from __future__ import annotations

from enum import IntEnum


# NOTE: this is actually the Medallion Layers
class DataLayer(IntEnum):
    RAW = 0  # Bronze Layer
    CLEANED = 1  # Silver Layer
    CURATED = 2  # Gold Layer

    def __str__(self):
        return self.name

    __repr__ = __str__

    @classmethod
    def parse(cls, value: DataLayer | str) -> DataLayer:
        """Parses a data layer name (case-insensitive), e.g. 'raw', or returns the member as is."""
        if isinstance(value, str):
            try:
                return cls[value.upper()]
            except KeyError:
                raise ValueError(f"invalid data layer '{value}', must be one of {[dl.name for dl in cls]}") from None
        return value
