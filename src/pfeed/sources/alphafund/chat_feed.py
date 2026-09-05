from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Literal, Self, Any, cast

if TYPE_CHECKING:
    from pfeed.dataflow.result import RunResult

from uuid import UUID

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
    AlphaFundChatFeedChannelDownloadRequest,
    AlphaFundChatFeedChatDownloadRequest,
    AlphaFundChatFeedMessageDownloadRequest,
    AlphaFundChatFeedRetrieveRequest,
)


class AlphaFundChatFeed(AlphaFundMixin, AlphaFundBaseFeed):
    ChannelDataModel: ClassVar[type[AlphaFundChannelDataModel]] = (
        AlphaFundChannelDataModel
    )
    ChatDataModel: ClassVar[type[AlphaFundChatDataModel]] = AlphaFundChatDataModel
    MessageDataModel: ClassVar[type[AlphaFundMessageDataModel]] = (
        AlphaFundMessageDataModel
    )
    data_domain: ClassVar[DataCategory] = DataCategory.CHAT_DATA

    def save_channel(
        self,
        fund_id: UUID,
        channel_name: str,
        user_ids: list[UUID],
        agent_ids: list[UUID] | None = None,
        channel_id: UUID | None = None,
        channel_type: Literal["direct_message", "group_chat"] = "direct_message",
        is_deleted: bool | None = None,
        is_archived: bool | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """
        Save a channel to storage.
        Args:
            channel_id: if provided, it means update the existing channel
            is_deleted, is_archived: None leaves the stored flag untouched
        """
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundChatFeedChannelDownloadRequest(
            data_source=self.name,
            fund_id=fund_id,
            channel_name=channel_name,
            channel_id=channel_id,
            channel_type=channel_type,
            user_ids=user_ids,
            agent_ids=agent_ids or [],
            is_deleted=is_deleted,
            is_archived=is_archived,
            storage_config=storage_config,
            io_config=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def save_chat(
        self,
        channel_id: UUID,
        chat_name: str,
        chat_id: UUID | None = None,
        is_main: bool = False,
        parent_message_id: UUID | None = None,
        is_deleted: bool | None = None,
        is_archived: bool | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """Save a chat to a channel
        Args:
            chat_id: if provided, it means update the existing chat
        """
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundChatFeedChatDownloadRequest(
            data_source=self.name,
            channel_id=channel_id,
            chat_name=chat_name,
            chat_id=chat_id,
            is_main=is_main,
            parent_message_id=parent_message_id,
            is_deleted=is_deleted,
            is_archived=is_archived,
            storage_config=storage_config,
            io_config=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def save_message(
        self,
        chat_id: UUID,
        content: str,
        message_seq: int,
        author_id: UUID,
        author_role: Literal["user", "agent", "system"],
        message_type: Literal["text", "compaction"] = "text",
        start_message_id: UUID | None = None,
        end_message_id: UUID | None = None,
        message_id: UUID | None = None,
        is_deleted: bool | None = None,
        is_archived: bool | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """Save a message to a chat
        Args:
            author_id: user id, agent id, or fund id for a system message
            message_type: 'compaction' means the content stands in for the
                messages from start_message_id to end_message_id
            message_id: if provided, it means update the existing message
        """
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundChatFeedMessageDownloadRequest(
            data_source=self.name,
            chat_id=chat_id,
            content=content,
            message_seq=message_seq,
            author_id=author_id,
            author_role=author_role,
            message_type=message_type,
            start_message_id=start_message_id,
            end_message_id=end_message_id,
            message_id=message_id,
            is_deleted=is_deleted,
            is_archived=is_archived,
            storage_config=storage_config,
            io_config=io_config,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(extract_func=self._download_impl)
        return self.run() if not self.is_pipeline() else self

    def retrieve(
        self,
        fund_id: UUID | None = None,
        channel_id: UUID | None = None,
        chat_id: UUID | None = None,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> Self | RunResult:
        """Retrieve child chat data from storage.

        Args:
            fund_id: Retrieve all channels in this fund.
            channel_id: Retrieve all chats in this channel.
            chat_id: Retrieve all messages in this chat.
        """
        storage_config, io_config = self._resolve_configs(storage_config, io_config)
        request = AlphaFundChatFeedRetrieveRequest(
            data_source=self.name,
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

    def _retrieve_impl(
        self,
        data_model: AlphaFundChannelDataModel
        | AlphaFundChatDataModel
        | AlphaFundMessageDataModel,
        request: AlphaFundChatFeedRetrieveRequest,
    ) -> pl.LazyFrame | None:
        existing = self._read_from_storage(
            data_model,
            request.storage_config_for_retrieval,
            request.io_config_for_retrieval,
        )

        is_fund_channels_lookup = (
            isinstance(data_model, AlphaFundChannelDataModel)
            and data_model.fund_id is not None
            and data_model.channel_id is None
        )
        is_channel_chats_lookup = (
            isinstance(data_model, AlphaFundChatDataModel)
            and data_model.channel_id is not None
            and data_model.chat_id is None
        )
        is_chat_messages_lookup = (
            isinstance(data_model, AlphaFundMessageDataModel)
            and data_model.chat_id is not None
            # and data_model.message_id is None
        )

        # Collection lookups return an empty frame when no children are stored.
        if (
            is_fund_channels_lookup
            or is_channel_chats_lookup
            or is_chat_messages_lookup
        ):
            return (
                existing
                if existing is not None
                else pl.DataFrame(schema=data_model.polars_schema()).lazy()
            )

        raise RuntimeError(
            "Invalid chat lookup state: expected fund_id for channels, "
            + "channel_id for chats, or chat_id for messages"
        )

    def create_data_model(
        self,
        data_type: Literal["channel", "chat", "message"],
        *args: Any,
        **kwargs: Any,
    ) -> AlphaFundChannelDataModel | AlphaFundChatDataModel | AlphaFundMessageDataModel:
        kwargs.setdefault("data_source", self.data_source)
        if data_type == "channel":
            return self.ChannelDataModel(*args, **kwargs)
        elif data_type == "chat":
            return self.ChatDataModel(*args, **kwargs)
        elif data_type == "message":
            return self.MessageDataModel(*args, **kwargs)
        else:
            raise ValueError(f"Invalid data type: {data_type}")

    def _create_data_model_from_request(
        self,
        request: AlphaFundChatFeedChannelDownloadRequest
        | AlphaFundChatFeedChatDownloadRequest
        | AlphaFundChatFeedMessageDownloadRequest
        | AlphaFundChatFeedRetrieveRequest,
    ) -> AlphaFundChannelDataModel | AlphaFundChatDataModel | AlphaFundMessageDataModel:
        if isinstance(request, AlphaFundChatFeedChannelDownloadRequest):
            return self._create_channel_data_model_from_request(request)
        elif isinstance(request, AlphaFundChatFeedChatDownloadRequest):
            return self._create_chat_data_model_from_request(request)
        elif isinstance(request, AlphaFundChatFeedMessageDownloadRequest):
            return self._create_message_data_model_from_request(request)
        elif isinstance(request, AlphaFundChatFeedRetrieveRequest):
            if request.chat_id is not None:
                data_model = self.create_data_model(
                    data_type="message",
                    chat_id=request.chat_id,
                )
            elif request.channel_id is not None:
                data_model = self.create_data_model(
                    data_type="chat",
                    channel_id=request.channel_id,
                )
            elif request.fund_id is not None:
                data_model = self.create_data_model(
                    data_type="channel",
                    fund_id=request.fund_id,
                )
            else:
                raise ValueError(
                    "One of fund_id, channel_id, or chat_id must be provided"
                )
            data_model.op = "read"
            return data_model
        else:
            raise ValueError(f"Invalid request type: {type(request)}")

    @staticmethod
    def _provided_flags(
        *,
        is_deleted: bool | None,
        is_archived: bool | None,
    ) -> dict[str, bool]:
        """Drop unset status flags so updates leave their stored values unchanged."""
        return {
            key: value
            for key, value in {
                "is_deleted": is_deleted,
                "is_archived": is_archived,
            }.items()
            if value is not None
        }

    def _create_channel_data_model_from_request(
        self,
        request: AlphaFundChatFeedChannelDownloadRequest,
    ) -> AlphaFundChannelDataModel:
        data_model = cast(
            AlphaFundChannelDataModel,
            self.create_data_model(
                data_type="channel",
                data_source=self.data_source,
                fund_id=request.fund_id,
                channel_name=request.channel_name,
                channel_id=request.channel_id,
                channel_type=request.channel_type,
                user_ids=request.user_ids,
                agent_ids=request.agent_ids,
                **self._provided_flags(
                    is_deleted=request.is_deleted,
                    is_archived=request.is_archived,
                ),
            ),
        )
        if isinstance(request, AlphaFundChatFeedRetrieveRequest):
            data_model.op = "read"
        elif isinstance(request, AlphaFundChatFeedChannelDownloadRequest):
            data_model.op = "create" if request.channel_id is None else "update"
        else:
            raise ValueError(f"Unknown request type: {type(request)}")
        return data_model

    def _create_chat_data_model_from_request(
        self,
        request: AlphaFundChatFeedChatDownloadRequest,
    ) -> AlphaFundChatDataModel:
        data_model = cast(
            AlphaFundChatDataModel,
            self.create_data_model(
                data_type="chat",
                data_source=self.data_source,
                channel_id=request.channel_id,
                chat_name=request.chat_name,
                chat_id=request.chat_id,
                is_main=request.is_main,
                parent_message_id=request.parent_message_id,
                **self._provided_flags(
                    is_deleted=request.is_deleted,
                    is_archived=request.is_archived,
                ),
            ),
        )
        if isinstance(request, AlphaFundChatFeedRetrieveRequest):
            data_model.op = "read"
        elif isinstance(request, AlphaFundChatFeedChatDownloadRequest):
            data_model.op = "create" if request.chat_id is None else "update"
        else:
            raise ValueError(f"Unknown request type: {type(request)}")
        return data_model

    def _create_message_data_model_from_request(
        self,
        request: AlphaFundChatFeedMessageDownloadRequest,
    ) -> AlphaFundMessageDataModel:
        data_model = cast(
            AlphaFundMessageDataModel,
            self.create_data_model(
                data_type="message",
                data_source=self.data_source,
                chat_id=request.chat_id,
                content=request.content,
                message_seq=request.message_seq,
                author_id=request.author_id,
                author_role=request.author_role,
                message_type=request.message_type,
                start_message_id=request.start_message_id,
                end_message_id=request.end_message_id,
                message_id=request.message_id,
                **self._provided_flags(
                    is_deleted=request.is_deleted,
                    is_archived=request.is_archived,
                ),
            ),
        )
        if isinstance(request, AlphaFundChatFeedRetrieveRequest):
            data_model.op = "read"
        elif isinstance(request, AlphaFundChatFeedMessageDownloadRequest):
            data_model.op = "create" if request.message_id is None else "update"
        else:
            raise ValueError(f"Unknown request type: {type(request)}")
        return data_model
