from pfeed.sources.base_data_source import BaseDataSource


__all__ = ["BybitDataSource"]


class BybitDataSource(BaseDataSource):
    def __init__(self):
        from pfeed.sources.bybit.api import BybitAPI
        super().__init__('bybit')
        self.start_date = self.specific_metadata['start_date']
        exchange = self._create_exchange()
        self.adapter = exchange.adapter
        self.PTYPE_TO_CATEGORY = exchange.PTYPE_TO_CATEGORY
        self.api = BybitAPI(exchange)
        
    @staticmethod
    def _create_exchange():
        from pfund.exchanges.bybit.exchange import Exchange
        return Exchange(env='LIVE')
        
    def get_products_by_ptypes(self, ptypes: list[str]) -> list[str]:
        '''Get products by product types
        e.g. if ptype='PERP', return all perpetual products
        '''
        return [
            self.adapter(epdt, ref_key=self.PTYPE_TO_CATEGORY[ptype]) 
            for ptype in ptypes 
            for epdt in self.api.get_epdts(ptype)
            # NOTE: if adapter(epdt, ref_key=category) == epdt, i.e. key is not found in pdt matching, meaning the product has been delisted
            if self.adapter(epdt, ref_key=self.PTYPE_TO_CATEGORY[ptype]) != epdt
        ]
        
    def download_market_data(self, pdt: str, date: str) -> bytes | None:
        raw_data: bytes | None = self.api.get_data(pdt, date)
        return raw_data
