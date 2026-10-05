from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot

    from pfeed.engine import DataEngine
    from pfeed.io.ducklake_io import DuckLakeIO
    from pfeed.io.parquet_io import ParquetIO
    from pfeed.sources.alphafund import AlphaFund
    from pfeed.sources.bybit import Bybit
    from pfeed.sources.crypto_hft_data import (
        CryptoHftData,
        CryptoHftData as CHD,
        CryptoHftData as CryptoHFTData,
    )
    from pfeed.sources.fxmacrodata import FXMacroData
    from pfeed.sources.pfund import PFund
    from pfeed.utils.aliases import ALIASES as alias  # noqa: N811

from pfeed.config import configure, configure_logging, get_config


def __getattr__(name: str):
    if name == "__version__":
        from importlib.metadata import version

        return version("pfeed")
    elif name == "alias":
        from pfeed.utils.aliases import ALIASES

        return ALIASES
    elif name == "plot":
        import pfund_plot as plot

        return plot
    elif name == "ParquetIO":
        from pfeed.io.parquet_io import ParquetIO

        return ParquetIO
    elif name == "DuckLakeIO":
        from pfeed.io.ducklake_io import DuckLakeIO

        return DuckLakeIO
    elif name == "DataEngine":
        from pfeed.engine import DataEngine

        return DataEngine
    elif name.lower() == "bybit":
        from pfeed.sources.bybit import Bybit

        return Bybit
    elif name.lower() in ("cryptohftdata", "chd"):
        from pfeed.sources.crypto_hft_data import CryptoHftData

        return CryptoHftData
    elif name.lower() == "pfund":
        from pfeed.sources.pfund import PFund

        return PFund
    elif name.lower() == "alphafund":
        from pfeed.sources.alphafund import AlphaFund

        return AlphaFund
    elif name.lower() in ("fxmacrodata", "fxmd"):
        from pfeed.sources.fxmacrodata import FXMacroData

        return FXMacroData
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = (  # noqa: RUF022
    "alias",
    "get_config", "configure", "configure_logging",
    "plot",
    "DataEngine",
    # IOs
    "DuckLakeIO",
    "ParquetIO",
    # Data Sources
    "AlphaFund",
    "PFund",
    "Bybit",
    "CHD", "CryptoHFTData", "CryptoHftData",
    "FXMacroData",
)


def __dir__():
    return sorted(__all__)
