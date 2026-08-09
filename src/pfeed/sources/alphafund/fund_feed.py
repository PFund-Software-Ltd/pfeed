from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

    from pfeed.dataflow.result import DataFlowResult, RunResult
    from pfeed.io.io_config import IOConfig
    from pfeed.sources.alphafund.requests.fund_base_request import (
        AlphaFundFeedBaseRequest,
    )
    from pfeed.sources.alphafund.requests.fund_download_request import (
        AlphaFundFeedDownloadRequest,
    )
    from pfeed.sources.alphafund.requests.fund_retrieve_request import (
        AlphaFundFeedRetrieveRequest,
    )
    from pfeed.storages.storage_config import StorageConfig

from uuid import UUID

import polars as pl

from pfeed.enums import DataCategory, DataStorage, IOFormat
from pfeed.feeds.base_feed import BaseFeed
from pfeed.io.io_config import IOConfig
from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.storages.database_storage import DatabaseStorage
from pfeed.storages.storage_config import StorageConfig


class AlphaFundFeed(AlphaFundMixin, BaseFeed):
    DataModel: ClassVar[type[AlphaFundDataModel]] = AlphaFundDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.FUND_DATA

    def _append_request(self, request: AlphaFundFeedBaseRequest) -> None:
        if self._requests:
            raise ValueError(f"{self.name} can only run one request at a time")
        return super()._append_request(request)

    def download(
        self,
        user_id: UUID,
        fund_name: str = "AlphaFund",
        fund_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        from pfeed.sources.alphafund.requests.fund_download_request import (
            AlphaFundFeedDownloadRequest,
        )

        storage_config = self._normalize_storage_config(
            storage_config or StorageConfig(storage=DataStorage.SQLITE)
        )
        io_config = self._normalize_io_config(
            io_config or IOConfig(io_format=IOFormat.SQLITE)
        )
        request = AlphaFundFeedDownloadRequest(
            data_source=self.name,
            user_id=user_id,
            fund_name=fund_name,
            fund_id=fund_id,
            storage_config=storage_config,
            io_config=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def _download_impl(self, data_model: AlphaFundDataModel) -> pl.DataFrame:
        return data_model.to_frame()

    def _get_default_transformations_for_download(
        self, request: AlphaFundFeedDownloadRequest
    ) -> list[Callable[..., Any]]:
        from pfeed._etl.base import convert_dataframe
        from pfeed.config import get_config
        from pfeed.utils import lambda_with_name

        config = get_config()

        default_transformations = [
            lambda_with_name(
                "convert_to_user_df",
                lambda df: convert_dataframe(df, data_tool=config.data_tool),
            ),
        ]
        return default_transformations

    def retrieve(
        self,
        user_id: UUID,
        fund_name: str = "AlphaFund",
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        from pfeed.sources.alphafund.requests.fund_retrieve_request import (
            AlphaFundFeedRetrieveRequest,
        )

        storage_config = self._normalize_storage_config(
            storage_config or StorageConfig(storage=DataStorage.SQLITE)
        )
        io_config = self._normalize_io_config(
            io_config or IOConfig(io_format=IOFormat.SQLITE)
        )
        request = AlphaFundFeedRetrieveRequest(
            data_source=self.name,
            user_id=user_id,
            fund_name=fund_name,
            storage_config_for_retrieval=storage_config,
            io_config_for_retrieval=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(data_model, request)
        )
        return self.run() if not self.is_pipeline() else self

    def _retrieve_impl(
        self,
        data_model: AlphaFundDataModel,
        request: AlphaFundFeedRetrieveRequest,
    ) -> pl.LazyFrame | None:
        storage_config = request.storage_config_for_retrieval
        io_config = request.io_config_for_retrieval
        Storage = DataStorage(storage_config.storage).storage_class
        storage = (
            Storage.from_storage_config(storage_config)
            .with_io(io_config)
            .with_data_model(data_model)
        )
        if not isinstance(storage, DatabaseStorage):
            raise TypeError(f"{self.name} fund data requires database storage")
        return storage.read()

    def _get_default_transformations_for_retrieve(
        self,
        request: AlphaFundFeedRetrieveRequest,
    ) -> list[Callable[..., Any]]:
        return self._get_default_transformations_for_download(request)

    def create_data_model(
        self,
        user_id: UUID,
        fund_name: str = "AlphaFund",
        fund_id: UUID | None = None,
    ) -> AlphaFundDataModel:
        data = {
            "data_source": self.data_source,
            "data_origin": "",
            "user_id": user_id,
            "fund_name": fund_name,
        }
        if fund_id is not None:
            data["fund_id"] = fund_id
        return self.DataModel.model_validate(data)

    def _create_data_model_from_request(
        self,
        request: AlphaFundFeedBaseRequest,
    ) -> AlphaFundDataModel:
        return self.create_data_model(
            user_id=request.user_id,
            fund_name=request.fund_name,
            fund_id=request.fund_id,
        )

    def run(self, **prefect_kwargs: Any) -> RunResult:
        from pfeed._etl.base import convert_dataframe
        from pfeed.dataflow.result import RunResult

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
