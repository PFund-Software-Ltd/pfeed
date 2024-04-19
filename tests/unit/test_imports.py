import pytest
import pfeed as pe


def test_import_etl():
    try:
        from pfeed import etl
    except ImportError:
        pytest.fail("Failed to import etl module")
    
def test_import_bybit():
    try:
        from pfeed import bybit
    except ImportError:
        pytest.fail("Failed to import bybit module")

def test_import_binance():
    try:
        from pfeed import binance
    except ImportError:
        pytest.fail("Failed to import binance module")
    
def test_import_YahooFinanceFeed():
    try:
        from pfeed import YahooFinanceFeed
    except ImportError:
        pytest.fail("Failed to import YahooFinanceFeed class")
    
def test_import_BybitFeed():
    try:
        from pfeed import BybitFeed
    except ImportError:
        pytest.fail("Failed to import BybitFeed class")

def test_import_BinanceFeed():
    try:
        from pfeed import BinanceFeed
    except ImportError:
        pytest.fail("Failed to import BinanceFeed class")
    
@pytest.mark.smoke
def test_import_all():
    for attr in pe.__all__:
        assert hasattr(pe, attr), f"Package 'pfeed' does not have '{attr}'"
    