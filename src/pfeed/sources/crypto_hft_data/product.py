"""Venue-aware products for CryptoHFTData."""

from __future__ import annotations

from typing import Any

from pfund.entities.products.product_base import BaseProduct
from pydantic import Field


class CryptoHftDataProduct(BaseProduct):
    """A crypto instrument whose identity includes its source exchange."""

    exchange: str = Field(default=..., min_length=1)

    def _create_symbol(self) -> str:
        return self.base_asset + self.quote_asset

    def _create_name(self) -> str:
        return "_".join([str(self.source), self.exchange.upper(), self.symbol])

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other) and self.exchange == other.exchange

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.exchange))
