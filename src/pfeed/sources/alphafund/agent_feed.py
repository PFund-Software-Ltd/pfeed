from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.io.io_config import IOConfig

from uuid import NAMESPACE_DNS, UUID, uuid5

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
    def _ensure_unique_key(
        *, fund_id: UUID | None, agent_name: str | None, agent_id: UUID | None
    ) -> UUID:
        if fund_id is None or agent_name is None:
            if agent_id is None:
                raise ValueError(
                    "Either fund_id and agent_name must be provided, or agent_id must be provided"
                )
            else:
                return agent_id
        else:
            return create_agent_id(fund_id, agent_name)

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
        agent_id = self._ensure_unique_key(
            fund_id=fund_id, agent_name=agent_name, agent_id=agent_id
        )
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
        agent_id = self._ensure_unique_key(
            fund_id=fund_id, agent_name=agent_name, agent_id=agent_id
        )
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
        agent_id = self._ensure_unique_key(
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
