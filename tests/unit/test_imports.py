import pytest
import pfeed as pe


def test_import_etl():
    from pfeed import etl
    assert etl, 'Failed to import etl module'
    
def test_import_bybit():
    from pfeed import bybit
    assert bybit, 'Failed to import bybit module'
    
def test_import_YahooFinanceFeed():
    from pfeed import YahooFinanceFeed
    assert YahooFinanceFeed, 'Failed to import YahooFinanceFeed class'
    
def test_import_BybitFeed():
    from pfeed import BybitFeed
    assert BybitFeed, 'Failed to import BybitFeed class'

@pytest.mark.smoke
def test_import_all():
    for attr in pe.__all__:
        assert hasattr(pe, attr), f"Package 'pfeed' does not have '{attr}'"