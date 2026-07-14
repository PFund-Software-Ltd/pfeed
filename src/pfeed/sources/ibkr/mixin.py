# pyright: reportUninitializedInstanceVariable=false, reportUnusedParameter=false
from __future__ import annotations

from pfeed.sources.ibkr.source import InteractiveBrokersSource


class InteractiveBrokersMixin:
    data_source: InteractiveBrokersSource

    @staticmethod
    def _create_data_source() -> InteractiveBrokersSource:
        return InteractiveBrokersSource()
