from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult
    from pfeed.sources.alphafund.requests.chat_base_request import (
        AlphaFundChatFeedBaseRequest,
    )

from uuid import NAMESPACE_DNS, UUID, uuid5

from pfeed.enums import DataCategory
from pfeed.sources.alphafund.base_feed import AlphaFundBaseFeed
from pfeed.io.io_config import IOConfig
from pfeed.sources.alphafund.channel_data_model import AlphaFundChannelDataModel
from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
from pfeed.sources.alphafund.message_data_model import AlphaFundMessageDataModel
from pfeed.sources.alphafund.mixin import AlphaFundMixin
from pfeed.storages.storage_config import StorageConfig
from pfeed.sources.alphafund.requests import (
    AlphaFundChatFeedDownloadRequest,
    AlphaFundChatFeedRetrieveRequest,
)


CHANNEL_ID_NAMESPACE = uuid5(NAMESPACE_DNS, "channel.alphafund.pfund.ai")


def create_channel_id(fund_id: UUID, channel_name: str) -> UUID:
    return uuid5(CHANNEL_ID_NAMESPACE, f"{fund_id}:{channel_name}")


class AlphaFundChatFeed(AlphaFundMixin, AlphaFundBaseFeed):
    ChannelDataModel: ClassVar[type[AlphaFundChannelDataModel]] = (
        AlphaFundChannelDataModel
    )
    ChatDataModel: ClassVar[type[AlphaFundChatDataModel]] = AlphaFundChatDataModel
    MessageDataModel: ClassVar[type[AlphaFundMessageDataModel]] = (
        AlphaFundMessageDataModel
    )
    data_domain: ClassVar[DataCategory] = DataCategory.CHAT_DATA

    @staticmethod
    def _ensure_unique_key(
        *, fund_id: UUID | None, channel_name: str | None, channel_id: UUID | None
    ) -> UUID:
        if fund_id is None or channel_name is None:
            if channel_id is None:
                raise ValueError(
                    "Either fund_id and channel_name must be provided, or channel_id must be provided"
                )
            else:
                return channel_id
        else:
            channel_id = create_channel_id(fund_id, channel_name)
        return channel_id

    def download(
        self,
        fund_id: UUID | None = None,
        channel_name: str | None = None,
        channel_id: UUID | None = None,
        chat_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """Write an existing channel's chats and messages to storage.

        Identify the channel with ``channel_id``, or resolve
        ``(fund_id, channel_name) -> channel_id``. For direct-message channels,
        ``channel_name`` is the agent's ``agent_name``.

        Once the channel is resolved, omitting ``chat_id`` persists every chat
        and all of their messages in that channel. Providing ``chat_id``
        persists only that chat and its messages.
        """
        channel_id = self._ensure_unique_key(
            fund_id=fund_id, channel_name=channel_name, channel_id=channel_id
        )
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundChatFeedDownloadRequest(
            data_source=self.name,
            channel_name=channel_name,
            fund_id=fund_id,
            channel_id=channel_id,
            chat_id=chat_id,
            storage_config=storage_config,
            io_config=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def retrieve(
        self,
        fund_id: UUID | None = None,
        channel_name: str | None = None,
        channel_id: UUID | None = None,
        chat_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        channel_id = self._ensure_unique_key(
            fund_id=fund_id, channel_name=channel_name, channel_id=channel_id
        )
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundChatFeedRetrieveRequest(
            data_source=self.name,
            channel_name=channel_name,
            fund_id=fund_id,
            channel_id=channel_id,
            chat_id=chat_id,
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
        channel_name: str | None = None,
        channel_id: UUID | None = None,
        chat_id: UUID | None = None,
    ) -> AlphaFundChannelDataModel | AlphaFundChatDataModel:
        channel_id = self._ensure_unique_key(
            fund_id=fund_id, channel_name=channel_name, channel_id=channel_id
        )
        if chat_id is not None:
            return self.ChatDataModel(
                data_source=self.data_source,
                channel_id=channel_id,
                chat_id=chat_id,
            )
        else:
            return self.ChannelDataModel(
                data_source=self.data_source,
                fund_id=fund_id,
                channel_name=channel_name,
                channel_id=channel_id,
            )

    def _create_data_model_from_request(
        self,
        request: AlphaFundChatFeedBaseRequest,
    ) -> AlphaFundChannelDataModel | AlphaFundChatDataModel:
        return self.create_data_model(
            fund_id=request.fund_id,
            channel_name=request.channel_name,
            channel_id=request.channel_id,
            chat_id=request.chat_id,
        )
