# pyright: reportUninitializedInstanceVariable=false, reportUnusedParameter=false
from __future__ import annotations

from pfeed.sources.bybit.source import BybitSource


class BybitMixin:
    data_source: BybitSource

    @staticmethod
    def _create_data_source() -> BybitSource:
        return BybitSource()
