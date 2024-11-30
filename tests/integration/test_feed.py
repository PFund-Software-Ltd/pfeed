import pytest
from pfeed.feeds.bybit_feed import BybitFeed


@pytest.fixture
def bybit_feed(request):
    if hasattr(request, 'param'):
        return BybitFeed(**request.param)
    else:
        return BybitFeed()


@pytest.mark.parametrize('bybit_feed', [{'data_tool': 'polars', 'use_ray': False, 'use_prefect': False, 'pipeline_mode': True}], indirect=True)
def test_download_duplicate_data(bybit_feed):
    pdt, date = 'BTC_USDT_PERP', '2024-01-01'
    with pytest.raises(Exception) as exc_info:
        (
            bybit_feed
            .download(pdt, start_date=date, end_date=date)
            .download(pdt, start_date=date, end_date=date)
        )
    assert 'duplicated' in str(exc_info.value)