import pytest

import pfeed as pe
from tests.fixtures.common import IDS, PARAMS, build_mixed_params


@pytest.fixture(params=PARAMS, ids=IDS)
def yahoo_finance(request):
    """Uses polars (default) with all ray/deltalake combinations."""
    return pe.YahooFinance(**request.param)


MIXED_PARAMS, MIXED_IDS = build_mixed_params()

@pytest.fixture(params=MIXED_PARAMS, ids=MIXED_IDS)
def yahoo_finance_mixed(request):
    """Cycles through pandas/dask/spark with all ray/deltalake combinations."""
    return pe.YahooFinance(**request.param)
