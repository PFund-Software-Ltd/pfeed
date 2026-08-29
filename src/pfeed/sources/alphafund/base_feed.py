from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Any, ClassVar

if TYPE_CHECKING:
    import polars as pl
    from narwhals.typing import IntoFrame

    from pfeed.dataflow.result import DataFlowResult
    from pfeed.sources.alphafund.data_handler import AlphaFundSQLDataModel
    from pfeed.sources.alphafund.requests.fund_base_request import (
        AlphaFundFeedBaseRequest,
    )
    from pfeed.sources.alphafund.requests.agent_base_request import (
        AlphaFundAgentFeedBaseRequest,
    )
    from pfeed.sources.alphafund.requests.chat_base_request import (
        AlphaFundChatFeedBaseRequest,
    )
    from pfeed.sources.alphafund.requests.fund_retrieve_request import (
        AlphaFundFeedRetrieveRequest,
    )
    from pfeed.sources.alphafund.requests.agent_retrieve_request import (
        AlphaFundAgentFeedRetrieveRequest,
    )
    from pfeed.sources.alphafund.requests.chat_retrieve_request import (
        AlphaFundChatFeedRetrieveRequest,
    )

    AlphaFundBaseRequest = (
        AlphaFundFeedBaseRequest
        | AlphaFundAgentFeedBaseRequest
        | AlphaFundChatFeedBaseRequest
    )
    # Only the retrieve requests carry the *_for_retrieval configs; each declares
    # them itself, so the union — not AlphaFundBaseRequest — is what types them.
    AlphaFundRetrieveRequest = (
        AlphaFundFeedRetrieveRequest
        | AlphaFundAgentFeedRetrieveRequest
        | AlphaFundChatFeedRetrieveRequest
    )

from pfeed.feeds.base_feed import BaseFeed
from pfeed.enums import DataStorage, IOFormat
from pfeed.enums.data_category import AlphaFundDataCategory
from pfeed.storages.database_storage import DatabaseStorage
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig
from pfeed.dataflow.result import RunResult


class AlphaFundBaseFeed(BaseFeed, ABC):
    data_domain: ClassVar[AlphaFundDataCategory]

    @abstractmethod
    def _ensure_unique_key(self, *args: Any, **kwargs: Any): ...

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

    def _append_request(self, request: AlphaFundBaseRequest) -> None:
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

    def _download_impl(
        self,
        data_model: AlphaFundSQLDataModel,
    ) -> pl.DataFrame | pl.LazyFrame:
        # _append_request() allows one request at a time, so the queued request is
        # this dataflow's. Reading it here keeps the signature to the single kwarg
        # Faucet.open_batch() passes, so feeds hand over _download_impl unbound.
        [request] = self._requests
        storage_config = request.storage_config
        io_config = request.io_config
        assert storage_config is not None
        assert io_config is not None
        existing = self._read_from_storage(data_model, storage_config, io_config)
        if existing is not None and not existing.limit(1).collect().is_empty():
            return existing
        return data_model.to_frame()

    def _retrieve_impl(
        self,
        data_model: AlphaFundSQLDataModel,
        request: AlphaFundRetrieveRequest,
    ) -> pl.LazyFrame | None:
        return self._read_from_storage(
            data_model,
            request.storage_config_for_retrieval,
            request.io_config_for_retrieval,
        )

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
