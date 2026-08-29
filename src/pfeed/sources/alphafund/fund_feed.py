from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.io.io_config import IOConfig

from uuid import NAMESPACE_DNS, UUID, uuid5

from pfeed.enums import DataCategory
from pfeed.sources.alphafund.base_feed import AlphaFundBaseFeed
from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.sources.alphafund.requests import (
    AlphaFundFeedDownloadRequest,
    AlphaFundFeedRetrieveRequest,
)


FUND_ID_NAMESPACE = uuid5(NAMESPACE_DNS, "fund.alphafund.pfund.ai")


def create_fund_id(user_id: UUID, fund_name: str) -> UUID:
    return uuid5(FUND_ID_NAMESPACE, f"{user_id}:{fund_name}")


class AlphaFundFeed(AlphaFundMixin, AlphaFundBaseFeed):
    DataModel: ClassVar[type[AlphaFundDataModel]] = AlphaFundDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.FUND_DATA

    @staticmethod
    def _ensure_unique_key(
        *, user_id: UUID | None, fund_name: str | None, fund_id: UUID | None
    ) -> UUID:
        if user_id is None or fund_name is None:
            if fund_id is None:
                raise ValueError(
                    "Either user_id and fund_name must be provided, or fund_id must be provided"
                )
            else:
                return fund_id
        else:
            fund_id = create_fund_id(user_id, fund_name)
        return fund_id

    def download(
        self,
        user_id: UUID | None = None,
        fund_name: str | None = None,
        fund_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """Persist a fund, or return the existing one for (user_id, fund_name).

        Args:
            fund_id: An existing deterministic ID. When omitted, it is derived
                from ``(user_id, fund_name)``.
        """
        fund_id = self._ensure_unique_key(
            user_id=user_id, fund_name=fund_name, fund_id=fund_id
        )
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
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

    def retrieve(
        self,
        user_id: UUID | None = None,
        fund_name: str | None = None,
        fund_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        fund_id = self._ensure_unique_key(
            user_id=user_id, fund_name=fund_name, fund_id=fund_id
        )
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundFeedRetrieveRequest(
            data_source=self.name,
            user_id=user_id,
            fund_name=fund_name,
            fund_id=fund_id,
            storage_config_for_retrieval=storage_config,
            io_config_for_retrieval=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(data_model, request)
        )
        return self.run() if not self.is_pipeline() else self

    def create_data_model(
        self,
        user_id: UUID | None = None,
        fund_name: str | None = None,
        fund_id: UUID | None = None,
    ) -> AlphaFundDataModel:
        fund_id = self._ensure_unique_key(
            user_id=user_id, fund_name=fund_name, fund_id=fund_id
        )
        return self.DataModel(
            data_source=self.data_source,
            user_id=user_id,
            fund_name=fund_name,
            fund_id=fund_id,
        )

    def _create_data_model_from_request(
        self,
        request: AlphaFundFeedDownloadRequest | AlphaFundFeedRetrieveRequest,
    ) -> AlphaFundDataModel:
        return self.create_data_model(
            user_id=request.user_id,
            fund_name=request.fund_name,
            fund_id=request.fund_id,
        )
