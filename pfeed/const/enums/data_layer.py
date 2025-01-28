from enum import IntEnum


# NOTE: this is actually the Medallion Layers
class DataLayer(IntEnum):
    CURATED = 0  # Gold Layer
    CLEANED = 1  # Silver Layer
    RAW = 2  # Bronze Layer