from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.enums.env import Environment
    from pfund_blocks.resolution import Resolution
    from pfund_blocks.typing import Message

    from pfeed.base.requests.stream_request import BaseStreamRequest

from abc import ABC, abstractmethod


class BaseStreamAPI(ABC):
    """A data source's streaming API, the only streaming interface pfeed talks to.

    A trading venue's implementation can be a thin wrapper of its websocket API(s),
    a data provider's implements the provider's streaming directly.
    """

    def __init__(self, env: Literal[Environment.PAPER, Environment.LIVE]):
        self._env = env
        self._callback: Callable[[Message], Awaitable[None] | None] | None = None

    @property
    def env(self) -> Literal[Environment.PAPER, Environment.LIVE]:
        return self._env

    @abstractmethod
    def get_supported_resolutions(self, product: BaseProduct) -> list[Resolution]:
        """Returns the resolutions the data source natively streams for the product."""
        ...

    @abstractmethod
    def add_channel(self, request: BaseStreamRequest) -> None:
        """Adds the product's market data channel, must be called before connect()."""
        ...

    def set_callback(self, callback: Callable[[Message], Awaitable[None] | None], *, skip_parse_message: bool = False) -> None:
        """Sets callback function that receives messages from the streaming api (e.g. websocket API).

        Message["message"] must be decoded. Message["data"] is None if not parsed.
        A data source's error must still be set in Message["error"], e.g. a rejected subscription.

        Args:
            skip_parse_message: if True, the messages must not be parsed, i.e. Message["data"] is None,
                e.g. when parsing is outsourced to the T (transformation functions) in Ray workers.
        """
        self._callback = callback

    @abstractmethod
    async def connect(self) -> None:
        """Starts connecting to the added channels in the background and returns right away, before they are connected."""
        ...

    @abstractmethod
    async def wait_until_disconnected(self) -> None:
        """Waits until the stream started by connect() has stopped, returns right away if never connected.

        Raises if the stream failed.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnects from the data source, so that wait_until_disconnected() returns."""
        ...
