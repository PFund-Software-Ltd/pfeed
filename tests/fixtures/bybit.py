import pytest

import pfeed as pe

from tests.fixtures.common import PARAMS, IDS, build_mixed_params


@pytest.fixture(params=PARAMS, ids=IDS)
def bybit(request):
    """Uses polars (default) with all ray/deltalake combinations."""
    return pe.Bybit(**request.param)


MIXED_PARAMS, MIXED_IDS = build_mixed_params()
@pytest.fixture(params=MIXED_PARAMS, ids=MIXED_IDS)
def bybit_mixed(request):
    """Cycles through pandas/dask/spark with all ray/deltalake combinations."""
    return pe.Bybit(**request.param)