# pyright: reportArgumentType=false
from __future__ import annotations
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, cast
if TYPE_CHECKING:
    from pfund.enums.env import Environment
    from pfeed.sources.yahoo_finance.product import YahooFinanceProduct

import yfinance

from pfeed.enums import DataSource, DataCategory, DataType, DataProviderType, DataAccessType
from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.sources.source_metadata import SourceMetadata
from pfeed.sources.yahoo_finance.stream_api import StreamAPI


__all__ = ["YahooFinanceSource"]


class YahooFinanceSource(DataProviderSource):
    NAME: ClassVar[DataSource] = DataSource.YAHOO_FINANCE
    METADATA: ClassVar[SourceMetadata] = SourceMetadata(
        data_origin="https://www.csidata.com",
        data_categories={
            DataCategory.MARKET_DATA: {
                DataType.MINUTE: ["STOCK", "ETF", "FUND", "OPTION", "FUTURE", "FOREX", "CRYPTO", "INDEX"],
                DataType.HOUR: ["STOCK", "ETF", "FUND", "OPTION", "FUTURE", "FOREX", "CRYPTO", "INDEX"],
                DataType.DAY: ["STOCK", "ETF", "FUND", "OPTION", "FUTURE", "FOREX", "CRYPTO", "INDEX"],
            },
        },
        provider_type=DataProviderType.AGGREGATOR,
        access_type=DataAccessType.FREE,
        api_key_required=False,
        github_repo="https://github.com/ranaroussi/yfinance",
        is_repo_official=False,
        docs_url="https://ranaroussi.github.io/yfinance/",
    )

    def get_batch_api(self) -> ModuleType:
        self._batch_api = yfinance
        return self._batch_api

    def get_stream_api(self, env: Environment | str | None=None) -> StreamAPI:
        '''Creates or reuses existing stream API for the given environment'''
        if self._stream_api is None:
            assert env is not None, "env must be provided when creating stream API"
            self._stream_api = StreamAPI()
        return self._stream_api

    def create_product(self, basis: str, symbol: str='', **specs: Any) -> YahooFinanceProduct:
        from pfund.entities.products import ProductFactory
        Product = cast(
            "type[YahooFinanceProduct]",
            ProductFactory(source=self.name, basis=basis)
        )
        product = Product(
            source=self.name,
            basis=basis,
            specs=specs,
            symbol=symbol,
        )
        return product
