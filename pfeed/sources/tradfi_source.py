from pfund.products.product_base import BaseProduct
from pfeed.sources.base_source import BaseSource


class TradFiSource(BaseSource):
    def create_product(self, basis: str, symbol='', **specs) -> BaseProduct:
        from pfund.products import ProductFactory
        # HACK: use 'IB' as trading venue to work around pydantic model's validation
        trading_venue = 'IB'
        Product = ProductFactory(trading_venue=trading_venue, basis=basis)
        product = Product(
            trading_venue=trading_venue,
            broker=trading_venue,
            # avoid missing "exchange" error in IBProduct, since in real trading, exchange must be provided for some asset types, e.g. futures
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