from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    import polars as pl
    from narwhals.typing import IntoFrame

    from pfeed.dataflow.result import DataFlowResult
    from pfeed.sources.alphafund.data_handler import AlphaFundSQLDataModel
    from pfeed.sources.alphafund.requests.fund_retrieve_request import (
        AlphaFundFeedRetrieveRequest,
    )
    from pfeed.sources.alphafund.requests.agent_retrieve_request import (
        AlphaFundAgentFeedRetrieveRequest,
    )
    from pfeed.sources.alphafund.requests.chat_retrieve_request import (
        AlphaFundChatFeedRetrieveRequest,
    )
    from pfeed.sources.alphafund.requests.fund_download_request import (
        AlphaFundFeedDownloadRequest,
    )
    from pfeed.sources.alphafund.requests.agent_download_request import (
        AlphaFundAgentFeedDownloadRequest,
    )
    from pfeed.sources.alphafund.requests.chat_download_request import (
        AlphaFundChatFeedChannelDownloadRequest,
        AlphaFundChatFeedChatDownloadRequest,
        AlphaFundChatFeedMessageDownloadRequest,
    )

    AlphaFundRetrieveRequest = (
        AlphaFundFeedRetrieveRequest
        | AlphaFundAgentFeedRetrieveRequest
        | AlphaFundChatFeedRetrieveRequest
    )
    AlphaFundDownloadRequest = (
        AlphaFundFeedDownloadRequest
        | AlphaFundAgentFeedDownloadRequest
        | AlphaFundChatFeedChannelDownloadRequest
        | AlphaFundChatFeedChatDownloadRequest
        | AlphaFundChatFeedMessageDownloadRequest
    )

from abc import ABC

from pfeed.feeds.base_feed import BaseFeed
from pfeed.enums import DataStorage, IOFormat
from pfeed.enums.data_category import AlphaFundDataCategory
from pfeed.storages.database_storage import DatabaseStorage
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig
from pfeed.dataflow.result import RunResult


class AlphaFundBaseFeed(BaseFeed, ABC):
    data_domain: ClassVar[AlphaFundDataCategory]

    def _resolve_configs(
        self,
        storage_config: StorageConfig | None,
        io_config: IOConfig | None,
    ) -> tuple[StorageConfig, IOConfig]:
        storage_config = self._normalize_storage_config(
            storage_config or StorageConfig(storage=DataStorage.SQLITE)
        )
        io_config = self._normalize_io_config(
            io_config or IOConfig(io_format=IOFormat.SQLITE)
        )
        return storage_config, io_config

    def _append_request(
        self, request: AlphaFundRetrieveRequest | AlphaFundDownloadRequest
    ) -> None:
        if self._requests:
            raise ValueError(f"{self.name} can only run one request at a time")
        return super()._append_request(request)

    def _read_from_storage(
        self,
        data_model: AlphaFundSQLDataModel,
        storage_config: StorageConfig,
        io_config: IOConfig,
    ) -> pl.LazyFrame | None:
        Storage = DataStorage(storage_config.storage).storage_class
        storage = (
            Storage.from_storage_config(storage_config)
            .with_io(io_config)
            .with_data_model(data_model)
        )
        if not isinstance(storage, DatabaseStorage):
            raise TypeError(f"{self.name} {self.data_domain} requires database storage")
        return storage.read()

    def _download_impl(self, data_model: AlphaFundSQLDataModel) -> pl.DataFrame:
        return data_model.to_frame()

    def run(self, **prefect_kwargs: Any) -> RunResult:
        from pfeed._etl.base import convert_dataframe

        dataflows = self._run_batch_dataflows(prefect_kwargs=prefect_kwargs)
        # one request only (enforced in _append_request),
        # so there is exactly one dataflow — nothing to aggregate.
        [dataflow] = dataflows
        result: DataFlowResult = dataflow.result
        data: IntoFrame | bytes | None = result.data
        # NOTE: only data artifact returns dataframe
        is_dataframe = data is not None and not isinstance(data, bytes)
        if is_dataframe:
            data = convert_dataframe(data)
        return RunResult(data=data, dataflows=dataflows)
