import pytest

import pfeed as pe
from tests.params.data_client import PARAMS


@pytest.fixture(params=PARAMS)
def yahoo_finance_all(request):
    return pe.YahooFinance(**request.param)


@pytest.fixture(params=[
    {'data_tool': 'polars', 'pipeline_mode': False, 'use_ray': False, 'use_prefect': False, 'use_deltalake': False},
    # TODO
    # {'data_tool': 'pandas', 'pipeline_mode': True, 'use_ray': False, 'use_prefect': False, 'use_deltalake': False},
    # {'data_tool': 'dask', 'pipeline_mode': False, 'use_ray': True, 'use_prefect': False, 'use_deltalake': False},
    # {'data_tool': 'spark', 'pipeline_mode': False, 'use_ray': False, 'use_prefect': True, 'use_deltalake': False},
    # {'data_tool': 'polars', 'pipeline_mode': False, 'use_ray': False, 'use_prefect': False, 'use_deltalake': True},
    # {'data_tool': 'pandas', 'pipeline_mode': True, 'use_ray': True, 'use_prefect': False, 'use_deltalake': True},
    # {'data_tool': 'dask', 'pipeline_mode': True, 'use_ray': True, 'use_prefect': True, 'use_deltalake': True},
    # {'data_tool': 'spark', 'pipeline_mode': False, 'use_ray': True, 'use_prefect': False, 'use_deltalake': True},
])
def yahoo_finance(request):
    return pe.YahooFinance(**request.param)
