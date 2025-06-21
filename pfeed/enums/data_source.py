from enum import StrEnum


class DataSource(StrEnum):
    YAHOO_FINANCE = 'YAHOO_FINANCE'
    FINANCIAL_MODELING_PREP = 'FINANCIAL_MODELING_PREP'
    BYBIT = 'BYBIT'
    BINANCE = 'BINANCE'
    # DATABENTO = 'DATABENTO'

    @property
    def data_client(self):
        import pfeed as pe
        from pfeed.utils.utils import to_camel_case
        return getattr(pe, to_camel_case(self))