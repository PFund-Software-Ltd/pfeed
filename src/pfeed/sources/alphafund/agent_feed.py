from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult
    from pfeed.io.io_config import IOConfig
    from pfeed.storages.storage_config import StorageConfig

from uuid import UUID

import polars as pl

from pfeed.enums import DataCategory
from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
from pfeed.sources.alphafund.base_feed import AlphaFundBaseFeed
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.sources.alphafund.requests import (
    AlphaFundAgentFeedDownloadRequest,
    AlphaFundAgentFeedRetrieveRequest,
)


class AlphaFundAgentFeed(AlphaFundMixin, AlphaFundBaseFeed):
    DataModel: ClassVar[type[AlphaFundAgentDataModel]] = AlphaFundAgentDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.AGENT_DATA

    def save_agent(
        self,
        agent_name: str,
        agent_role: str,
        agent_class: str,
        agent_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
        *,
        fund_id: UUID | None = None,
    ) -> Self | RunResult:
        """Save an agent to storage.
        Args:
            agent_id: if provided, it means update the existing agent
        """
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundAgentFeedDownloadRequest(
            data_source=self.name,
            fund_id=self._resolve_fund_id(fund_id),
            agent_name=agent_name,
            agent_role=agent_role,
            agent_class=agent_class,
            agent_id=agent_id,
            storage_config=storage_config,
            io_config=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def retrieve(
        self,
        fund_id: UUID | None = None,
        agent_name: str | None = None,
        agent_id: UUID | None = None,
        agent_role: str | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundAgentFeedRetrieveRequest(
            data_source=self.name,
            fund_id=self._resolve_fund_id(fund_id),
            agent_name=agent_name,
            agent_role=agent_role,
            agent_id=agent_id,
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
        data_model: AlphaFundAgentDataModel,
        request: AlphaFundAgentFeedRetrieveRequest,
    ) -> pl.LazyFrame | None:
        existing = self._read_from_storage(
            data_model,
            request.storage_config_for_retrieval,
            request.io_config_for_retrieval,
        )

        is_agents_lookup = data_model.agent_id is None and (
            data_model.fund_id is not None or data_model.agent_role is not None
        )
        is_unique_agent_lookup = data_model.agent_id is not None

        # Collection lookup: return zero or more agents.
        if is_agents_lookup:
            if existing is not None:
                return existing
            else:
                return (
                    pl.DataFrame(schema=data_model.polars_schema()).lazy()
                    if existing is None
                    else existing
                )

        # Unique-agent lookup: expect zero or one agent.
        if is_unique_agent_lookup:
            # the agent doesn't exist
            if existing is None:
                return None
            row_count = existing.limit(2).collect().height
            if row_count == 0:
                raise LookupError(f"Agent {data_model.agent_id} was not found")
            if row_count > 1:
                raise RuntimeError(
                    f"Expected one agent for {data_model.agent_id}, but multiple were found"
                )
            return existing

        raise RuntimeError(
            "Invalid agent lookup state: expected either an agent collection lookup "
            + "or a unique-agent lookup"
        )

    def create_data_model(
        self,
        fund_id: UUID | None = None,
        agent_name: str | None = None,
        agent_role: str | None = None,
        agent_class: str | None = None,
        agent_id: UUID | None = None,
    ) -> AlphaFundAgentDataModel:
        return self.DataModel(
            data_source=self.data_source,
            fund_id=self._resolve_fund_id(fund_id),
            agent_name=agent_name,
            agent_role=agent_role,
            agent_class=agent_class,
            agent_id=agent_id,
        )

    def _create_data_model_from_request(
        self,
        request: AlphaFundAgentFeedDownloadRequest | AlphaFundAgentFeedRetrieveRequest,
    ) -> AlphaFundAgentDataModel:
        agent_id = request.agent_id
        data_model = self.create_data_model(
            fund_id=request.fund_id,
            agent_name=request.agent_name,
            agent_role=request.agent_role,
            agent_class=(
                request.agent_class
                if isinstance(request, AlphaFundAgentFeedDownloadRequest)
                else None
            ),
            agent_id=agent_id,
        )
        if isinstance(request, AlphaFundAgentFeedRetrieveRequest):
            data_model.op = "read"
        elif isinstance(request, AlphaFundAgentFeedDownloadRequest):
            data_model.op = "create" if agent_id is None else "update"
        else:
            raise TypeError(f"Unknown request type: {request}")
        return data_model
