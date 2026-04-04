from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mtflow import WebSocketClient

from pfeed.sources.pfund.source import PFundSource


class PFundMixin:
    data_source: PFundSource  # pyright: ignore[reportUninitializedInstanceVariable]

    @property
    def ws_client(self) -> WebSocketClient:
        from mtflow import get_ws_client
        return get_ws_client()

    @staticmethod
    def _create_data_source() -> PFundSource:
        return PFundSource()