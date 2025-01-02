from enum import StrEnum


class DataTool(StrEnum):
    pandas = 'pandas'
    polars = 'polars'
    dask = 'dask'
    # spark = 'spark'
