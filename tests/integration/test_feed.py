import pytest
from pfeed.feeds.bybit.bybit_market_data_feed import BybitFeed


@pytest.fixture
def bybit_feed(request):
    if hasattr(request, 'param'):
        return BybitFeed(**request.param)
    else:
        return BybitFeed()
