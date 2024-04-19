import pytest
import pfeed as pe


def test_bybit_Feed_import():
    from pfeed import BybitFeed
    assert pe.bybit.Feed is pe.bybit.BybitFeed and pe.bybit.Feed is BybitFeed, \
        'Failed to import bybit.Feed class'

def test_bybit_api_import():
    try:
        from pfeed.sources.bybit import api
    except ImportError as e:
        pytest.fail(f"Failed to import bybit.api module: {str(e)}")

def test_bybit_name_access():
    assert hasattr(pe.bybit, 'name'), "Package 'pfeed.bybit' does not have 'name'"

def test_bybit_api_access():
    assert hasattr(pe.bybit, 'api'), "Package 'pfeed.bybit' does not have 'api'"

def test_bybit_download_access():
    assert hasattr(pe.bybit, 'download'), "Package 'pfeed.bybit' does not have 'download'"
    assert hasattr(pe.bybit, 'download_historical_data'), "Package 'pfeed.bybit' does not have 'download_historical_data'"
    
def test_bybit_stream_access():
    assert hasattr(pe.bybit, 'stream'), "Package 'pfeed.bybit' does not have 'stream'"
    assert hasattr(pe.bybit, 'stream_realtime_data'), "Package 'pfeed.bybit' does not have 'stream_realtime_data'"