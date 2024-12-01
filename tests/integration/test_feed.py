import pytest
from pfeed.feeds.bybit_feed import BybitFeed


@pytest.fixture
def bybit_feed(request):
    if hasattr(request, 'param'):
        return BybitFeed(**request.param)
    else:
        return BybitFeed()
