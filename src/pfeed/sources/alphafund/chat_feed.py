from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult
    from pfeed.sources.alphafund.requests.chat_base_request import (
        AlphaFundChatFeedBaseRequest,
    )

from uuid import NAMESPACE_DNS, UUID, uuid5

import polars as pl

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
    def _resolve_channel_id(
        *,
        fund_id: UUID | None,
        channel_name: str | None,
        channel_id: UUID | None,
        chat_id: UUID | None,
    ) -> UUID | None:
        if fund_id is not None and channel_name is not None:
            channel_id = create_channel_id(fund_id, channel_name)
        else:
            if channel_id is None:
                if fund_id is None:
                    raise ValueError("Either fund_id or channel_id must be provided")
                elif chat_id is not None:
                    raise ValueError(
                        "channel_id must be provided when chat_id is provided"
                    )
                else:
                    # NOTE: only fund_id is provided + channel_id is None = get all channels for that fund
                    return None
            else:
                return channel_id
        return channel_id

    def _handle_storage_result(
        self,
        data_model: AlphaFundChannelDataModel | AlphaFundChatDataModel,
        storage_config: StorageConfig,
        io_config: IOConfig,
    ) -> pl.LazyFrame | None:
        existing = self._read_from_storage(
            data_model,
            storage_config,
            io_config,
        )

        is_fund_channels_lookup = (
            isinstance(data_model, AlphaFundChannelDataModel)
            and data_model.fund_id is not None
            and data_model.channel_id is None
        )
        is_unique_channel_lookup = (
            isinstance(data_model, AlphaFundChannelDataModel)
            and data_model.channel_id is not None
        )
        is_unique_chat_lookup = (
            isinstance(data_model, AlphaFundChatDataModel)
            and data_model.channel_id is not None  # pyright: ignore[reportUnnecessaryComparison]
            and data_model.chat_id is not None  # pyright: ignore[reportUnnecessaryComparison]
        )

        # Fund-only lookup: return zero or more channels.
        if is_fund_channels_lookup:
            if existing is not None:
                return existing
            else:
                return (
                    pl.DataFrame(schema=data_model.polars_schema()).lazy()
                    if existing is None
                    else existing
                )

        # Unique-channel lookup: expect zero or one channel.
        if is_unique_channel_lookup:
            # the channel doesn't exist
            if existing is None:
                return None
            row_count = existing.limit(2).collect().height
            if row_count == 0:
                raise LookupError(f"Channel {data_model.channel_id} was not found")
            if row_count > 1:
                raise RuntimeError(
                    f"Expected one channel for {data_model.channel_id}, but multiple were found"
                )
            return existing

        # Unique-chat lookup: expect zero or one chat.
        if is_unique_chat_lookup:
            assert isinstance(data_model, AlphaFundChatDataModel)
            # the chat doesn't exist
            if existing is None:
                return None
            row_count = existing.limit(2).collect().height
            if row_count == 0:
                raise LookupError(f"Chat {data_model.chat_id} was not found")
            if row_count > 1:
                raise RuntimeError(
                    f"Expected one chat for {data_model.chat_id}, but multiple were found"
                )
            return existing

        raise RuntimeError(
            "Invalid chat lookup state: expected either a fund-only channel lookup, "
            + "a unique-channel lookup, or a unique-chat lookup"
        )

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
        channel_id = self._resolve_channel_id(
            fund_id=fund_id,
            channel_name=channel_name,
            channel_id=channel_id,
            chat_id=chat_id,
        )
        if channel_id is not None and chat_id is not None:
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
