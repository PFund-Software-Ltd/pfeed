import pytest
import pfeed as pe
    
    
def test_Feed_import():
    from pfeed import BinanceFeed
    assert pe.binance.Feed is pe.binance.BinanceFeed and pe.binance.Feed is BinanceFeed, \
        'Failed to import binance.Feed class'
        
def test_api_import():
    try:
        from pfeed.sources.binance import api
    except ImportError as e:
        pytest.fail(f"Failed to import binance.api module: {str(e)}")

def test_attributes_access():
    assert hasattr(pe.binance, 'name'), "Package 'pfeed.binance' does not have 'name'"
    assert hasattr(pe.binance, 'api'), "Package 'pfeed.binance' does not have 'api'"
    assert hasattr(pe.binance, 'download'), "Package 'pfeed.binance' does not have 'download'"
    assert hasattr(pe.binance, 'download_historical_data'), "Package 'pfeed.binance' does not have 'download_historical_data'"
    assert hasattr(pe.binance, 'stream'), "Package 'pfeed.binance' does not have 'stream'"
    assert hasattr(pe.binance, 'stream_realtime_data'), "Package 'pfeed.binance' does not have 'stream_realtime_data'"