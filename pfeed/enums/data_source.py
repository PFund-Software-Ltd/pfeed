from enum import StrEnum

from pfund_kit.utils.text import to_camel_case


class DataSource(StrEnum):
    YAHOO_FINANCE = YF = 'YAHOO_FINANCE'
    FINANCIAL_MODELING_PREP = FMP = 'FINANCIAL_MODELING_PREP'
    BYBIT = 'BYBIT'
    # BINANCE = 'BINANCE'
    # INTERACTIVE_BROKERS = IB = 'INTERACTIVE_BROKERS'
    # DATABENTO = 'DATABENTO'

    @property
    def data_client_class(self):
        import pfeed as pe
        return getattr(pe, to_camel_case(self))
    
    @property
    def data_source_class(self):
        import importlib
        return getattr(importlib.import_module(f'pfeed.sources.{self.lower()}.source'), f'{to_camel_case(self)}Source')
    