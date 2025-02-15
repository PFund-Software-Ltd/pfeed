from enum import IntEnum


# NOTE: this is actually the Medallion Layers
class DataLayer(IntEnum):
    RAW = 0  # Bronze Layer
    CLEANED = 1  # Silver Layer
    CURATED = 2  # Gold Layer