import itertools

import pytest
import pfeed as pe
from pfeed.enums import DataTool


# build a cycle iterator excluding 'polars'
_data_tools_cycle = itertools.cycle(
    [name for name in DataTool.__members__ if name != "polars"]
)

def next_data_tool():
    return next(_data_tools_cycle)


PARAMS = [
    {'use_ray': False, 'use_deltalake': False},
    {'use_ray': True, 'use_deltalake': False},
    {'use_ray': False, 'use_deltalake': True},
    {'use_ray': True, 'use_deltalake': True},
]


IDS = [
    "ray(F)delta(F)",
    "ray(T)delta(F)",
    "ray(F)delta(T)",
    "ray(T)delta(T)",
]


MIXED_PARAMS = []
MIXED_IDS = []
for param, _id in zip(PARAMS, IDS):
    data_tool = next_data_tool()
    param = {'data_tool': data_tool, **param}
    _id = data_tool + ':' + _id
    MIXED_PARAMS.append(param)
    MIXED_IDS.append(_id)


@pytest.fixture(params=MIXED_PARAMS, ids=MIXED_IDS)
def yahoo_finance_mixed(request):
    """YahooFinance fixture that cycles through different data tools (excluding polars) with all ray/deltalake combinations."""
    return pe.YahooFinance(**request.param)


@pytest.fixture(params=PARAMS, ids=IDS)
def yahoo_finance(request):
    return pe.YahooFinance(**request.param)
