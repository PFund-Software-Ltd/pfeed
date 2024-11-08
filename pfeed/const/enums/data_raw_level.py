from enum import StrEnum


class DataRawLevel(StrEnum):
    CLEANED = 'CLEANED'
    NORMALIZED = 'NORMALIZED'
    ORIGINAL = 'ORIGINAL'
