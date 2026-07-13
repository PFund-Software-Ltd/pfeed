from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # need these imports to support IDE hints:
    import pfund_plot as plot

    from pfeed._io.io_config import IOConfig
    from pfeed._sinks.sink_config import SinkConfig
    from pfeed.engine import DataEngine
    from pfeed.sources.alphafund import AlphaFund
    from pfeed.sources.bybit import Bybit
    from pfeed.sources.crypto_hft_data import CryptoHftData
    from pfeed.sources.crypto_hft_data import CryptoHftData as CHD  # noqa: N817
    from pfeed.sources.crypto_hft_data import CryptoHftData as CryptoHFTData
    from pfeed.sources.pfund import PFund
    from pfeed.sources.yahoo_finance import (
        YahooFinance,
    )
    from pfeed.sources.yahoo_finance import (
        YahooFinance as YF,  # noqa: N817
    )
    from pfeed.sources.yahoo_finance import (
        YahooFinance as YFinance,
    )
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.utils.aliases import ALIASES as alias  # noqa: N811
    # from pfeed.sources.financial_modeling_prep import (
    #     FinancialModelingPrep,
    #     FinancialModelingPrep as FMP,
    # )

import os

from pfeed.config import configure, configure_logging, get_config

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # used to suppress warning from pyspark
# disable this warning in Ray: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default).
os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"


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
    elif name == "StorageConfig":
        from pfeed.storages.storage_config import StorageConfig

        return StorageConfig
    elif name == "IOConfig":
        from pfeed._io.io_config import IOConfig

        return IOConfig
    elif name == "SinkConfig":
        from pfeed._sinks.sink_config import SinkConfig

        return SinkConfig
    elif name == "DataEngine":
        from pfeed.engine import DataEngine

        return DataEngine
    elif name.lower() in ("yahoofinance", "yfinance", "yf"):
        from pfeed.sources.yahoo_finance import YahooFinance

        return YahooFinance
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
    # elif name in ('FinancialModelingPrep', 'FMP'):
    #     from pfeed.sources.financial_modeling_prep import FinancialModelingPrep
    #     return FinancialModelingPrep
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = (
    "CHD",
    "YF",
    "AlphaFund",
    "Bybit",
    "CryptoHFTData",
    "CryptoHftData",
    "DataEngine",
    "IOConfig",
    "PFund",
    "SinkConfig",
    "StorageConfig",
    "YFinance",
    "YahooFinance",
    "alias",
    "configure",
    "configure_logging",
    "get_config",
    "plot",
)


def __dir__():
    return sorted(__all__)
