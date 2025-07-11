import pytest
from pfeed.sources.bybit.market_feed import BybitFeed


@pytest.fixture
def bybit_feed(request):
    if hasattr(request, 'param'):
        return BybitFeed(**request.param)
    else:
        return BybitFeed()
