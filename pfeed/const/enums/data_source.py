from enum import StrEnum


class DataSource(StrEnum):
    YAHOO_FINANCE = 'YAHOO_FINANCE'
    DATABENTO = 'DATABENTO'
    BYBIT = 'BYBIT'
    BINANCE = 'BINANCE'