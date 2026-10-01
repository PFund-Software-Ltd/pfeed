from enum import StrEnum

from pfund.enums.venue import TradingVenue
from pfund_kit.utils.text import to_pascal_case


class DataSource(StrEnum):
    PFUND = "PFUND"
    ALPHAFUND = "ALPHAFUND"
    YAHOO_FINANCE = YF = "YAHOO_FINANCE"
    FINANCIAL_MODELING_PREP = FMP = "FINANCIAL_MODELING_PREP"
    FXMACRODATA = "FXMACRODATA"
    CRYPTO_HFT_DATA = CHD = "CRYPTO_HFT_DATA"
    BYBIT = TradingVenue.BYBIT
    IBKR = TradingVenue.IBKR
    # BINANCE = TradingVenue.BINANCE
    # DATABENTO = 'DATABENTO'

    @property
    def data_client_class(self):
        import pfeed as pe

        return getattr(pe, to_pascal_case(self))

    @property
    def data_source_class(self):
        import importlib

        return getattr(
            importlib.import_module(f"pfeed.sources.{self.lower()}.source"),
            f"{to_pascal_case(self)}Source",
        )

    @property
    def product_class(self):
        import importlib

        return getattr(
            importlib.import_module(f"pfeed.sources.{self.lower()}.product"),
            f"{to_pascal_case(self)}Product",
        )
