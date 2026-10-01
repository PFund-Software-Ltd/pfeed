from __future__ import annotations

from pfeed.sources.fxmacrodata.source import FXMacroDataSource


class FXMacroDataMixin:
    data_source: FXMacroDataSource

    @staticmethod
    def _create_data_source() -> FXMacroDataSource:
        return FXMacroDataSource()
