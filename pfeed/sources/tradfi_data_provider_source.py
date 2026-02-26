from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
if TYPE_CHECKING:
    from pfund.entities.products.product_ibkr import IBKRProduct

from pfund.enums import TradingVenue
from pfeed.sources.data_provider_source import DataProviderSource


class TradFiDataProviderSource(DataProviderSource):
    def create_product(self, basis: str, symbol: str='', **specs: Any) -> IBKRProduct:
        from pfund.entities.products import ProductFactory
        from pfund.entities.products.product_ibkr import IBKRProduct  # pyright: ignore[reportUnusedImport]
        # HACK: use 'IB' as trading venue to work around pydantic model's validation
        trading_venue = TradingVenue.IBKR
        Product = cast(type[IBKRProduct], ProductFactory(trading_venue=trading_venue, basis=basis))
        product = Product(
            trading_venue=trading_venue,
            broker=trading_venue,
            # avoid missing "exchange" error in IBKRProduct, since in real trading, exchange must be provided for some asset types, e.g. futures
            exchange='SMART',
            basis=basis,
            specs=specs,
            symbol=symbol,
        )
        # HACK: write it back to data source's name
        # data_source_name = self.name.value
        # product.trading_venue = data_source_name
        # product.broker = data_source_name
        # # recreate symbol and name after changing trading venue and broker
        # product.specs = product._create_specs()
        # product.symbol = product._create_symbol()
        # product.name = product._create_name()
        return product