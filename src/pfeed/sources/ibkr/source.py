from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from pfund.venues.ibkr.product import InteractiveBrokersProduct

from pfund.enums.env import Environment

from pfeed.enums import (
    DataProviderType,
    DataSource,
)
from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.sources.ibkr.stream_api import StreamAPI
from pfeed.sources.source_metadata import SourceMetadata


class InteractiveBrokersSource(DataProviderSource):
    name: ClassVar[DataSource] = DataSource.IBKR
    METADATA: ClassVar[SourceMetadata] = SourceMetadata(
        # NOTE: technically IBKR is a venue, but its data has been aggregated
        provider_type=DataProviderType.AGGREGATOR,
    )

    # TODO: see if it's worth creating a BatchAPI to download historical data
    # def get_batch_api(self) -> BatchAPI:
    #     if self._batch_api is None:
    #         self._batch_api = BatchAPI()
    #     return self._batch_api

    def get_stream_api(
        self,
        env: Literal[
            Environment.PAPER,
            Environment.LIVE,
            "PAPER",
            "LIVE",
        ]
        | None = None,
    ) -> StreamAPI:
        if self._stream_api is None:
            if env is None:
                raise ValueError("env must be provided when creating stream API")
            self._stream_api = StreamAPI(env=env)
        return self._stream_api

    def create_product(
        self, basis: str, symbol: str = "", **specs: Any
    ) -> InteractiveBrokersProduct:
        from pfund.venues.ibkr.venue import InteractiveBrokers

        return InteractiveBrokers.create_product(basis, symbol=symbol, **specs)
