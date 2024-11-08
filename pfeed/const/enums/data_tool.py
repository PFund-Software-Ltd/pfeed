from enum import StrEnum


class DataTool(StrEnum):
    PANDAS = 'PANDAS'
    POLARS = 'POLARS'
    DASK = 'DASK'
    SPARK = 'SPARK'