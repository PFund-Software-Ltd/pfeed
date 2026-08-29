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
from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.sources.alphafund.requests import (
    AlphaFundAgentFeedDownloadRequest,
    AlphaFundAgentFeedRetrieveRequest,
)


AGENT_ID_NAMESPACE = uuid5(NAMESPACE_DNS, "agent.alphafund.pfund.ai")


def create_agent_id(fund_id: UUID, agent_name: str) -> UUID:
    return uuid5(AGENT_ID_NAMESPACE, f"{fund_id}:{agent_name}")


class AlphaFundAgentFeed(AlphaFundMixin, AlphaFundBaseFeed):
    DataModel: ClassVar[type[AlphaFundAgentDataModel]] = AlphaFundAgentDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.AGENT_DATA

    @staticmethod
    def _resolve_agent_id(
        *, fund_id: UUID | None, agent_name: str | None, agent_id: UUID | None
    ) -> UUID | None:
        if fund_id is not None and agent_name is not None:
            return create_agent_id(fund_id, agent_name)
        else:
            if agent_id is None:
                if fund_id is None:
                    raise ValueError("Either fund_id or agent_id must be provided")
                else:
                    # NOTE: only fund_id is provided + agent_id is None = get all agents for that fund
                    return None
            else:
                return agent_id

    def _handle_storage_result(
        self,
        data_model: AlphaFundAgentDataModel,
        storage_config: StorageConfig,
        io_config: IOConfig,
    ) -> pl.LazyFrame | None:
        existing = self._read_from_storage(
            data_model,
            storage_config,
            io_config,
        )

        is_fund_agents_lookup = (
            data_model.fund_id is not None and data_model.agent_id is None
        )
        is_unique_agent_lookup = data_model.agent_id is not None

        # Fund-only lookup: return zero or more agents.
        if is_fund_agents_lookup:
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
            "Invalid agent lookup state: expected either a fund-only lookup "
            + "or a unique-agent lookup"
        )

    def download(
        self,
        fund_id: UUID | None = None,
        agent_name: str | None = None,
        agent_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """Load or create the persistent identity for an agent.

        The authoritative key is ``(fund_id, agent_name)``. When omitted,
        ``agent_id`` is derived deterministically from that key.
        """
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundAgentFeedDownloadRequest(
            data_source=self.name,
            fund_id=fund_id,
            agent_name=agent_name,
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
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundAgentFeedRetrieveRequest(
            data_source=self.name,
            fund_id=fund_id,
            agent_name=agent_name,
            agent_id=agent_id,
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
        fund_id: UUID | None = None,
        agent_name: str | None = None,
        agent_id: UUID | None = None,
    ) -> AlphaFundAgentDataModel:
        agent_id = self._resolve_agent_id(
            fund_id=fund_id, agent_name=agent_name, agent_id=agent_id
        )
        return self.DataModel(
            data_source=self.data_source,
            fund_id=fund_id,
            agent_name=agent_name,
            agent_id=agent_id,
        )

    def _create_data_model_from_request(
        self,
        request: AlphaFundAgentFeedDownloadRequest | AlphaFundAgentFeedRetrieveRequest,
    ) -> AlphaFundAgentDataModel:
        return self.create_data_model(
            fund_id=request.fund_id,
            agent_name=request.agent_name,
            agent_id=request.agent_id,
        )
