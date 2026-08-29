from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Self, cast

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.io.io_config import IOConfig

from uuid import NAMESPACE_DNS, UUID, uuid5

import polars as pl

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
    def _resolve_fund_id(
        *, user_id: UUID | None, fund_name: str | None, fund_id: UUID | None
    ) -> UUID | None:
        if user_id is not None and fund_name is not None:
            fund_id = create_fund_id(user_id, fund_name)
        else:
            if fund_id is None:
                if user_id is None:
                    raise ValueError("Either user_id or fund_id must be provided")
                else:
                    # NOTE: only user_id is provided + fund_id is None = get all funds for that user
                    return None
            else:
                return fund_id
        return fund_id

    def _handle_storage_result(
        self,
        data_model: AlphaFundDataModel,
        storage_config: StorageConfig,
        io_config: IOConfig,
    ) -> pl.LazyFrame | None:
        existing = self._read_from_storage(
            data_model,
            storage_config,
            io_config,
        )

        is_user_funds_lookup = (
            data_model.user_id is not None and data_model.fund_id is None
        )
        is_unique_fund_lookup = data_model.fund_id is not None

        # User-only lookup: return zero or more funds.
        if is_user_funds_lookup:
            if existing is not None:
                return existing
            else:
                return (
                    pl.DataFrame(schema=data_model.polars_schema()).lazy()
                    if existing is None
                    else existing
                )

        # Unique-fund lookup: expect zero or one fund.
        if is_unique_fund_lookup:
            # the fund doesn't exist
            if existing is None:
                return None
            row_count = existing.limit(2).collect().height
            if row_count == 0:
                raise LookupError(f"Fund {data_model.fund_id} was not found")
            if row_count > 1:
                raise RuntimeError(
                    f"Expected one fund for {data_model.fund_id}, but multiple were found"
                )
            return existing

        raise RuntimeError(
            "Invalid fund lookup state: expected either a user-only lookup "
            + "or a unique-fund lookup"
        )

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
        fund_id = self._resolve_fund_id(
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
