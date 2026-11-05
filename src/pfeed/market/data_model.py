from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from pfeed.streaming.zeromq import ZeroMQChannel, ZeroMQTopic

from pfund_blocks.datas.bar_data import Bar
from pfund_blocks.datas.tick_data import Tick
from pfund_blocks.entities.products.product_base import BaseProduct
from pfund_blocks.enums.env import Environment
from pfund_blocks.resolution import Resolution
from pfund_blocks.typing import Message
from pydantic import field_serializer, field_validator

from pfeed.base.time_based_data_model import TimeBasedDataModel
from pfeed.market.data_handler import MarketDataHandler


# NOTE: Bar, Tick and Message are imported at runtime, msgspec resolves the type hints when decoding
class MarketDataMessage(Message):
    """A Message whose data is market data points, the type streamed market data is decoded into, see ZeroMQ.register_codec().

    A transformation adds its own fields to "extra", any other field would be dropped when decoding.
    """

    data: list[Bar | Tick] | None


class MarketDataModel(TimeBasedDataModel):
    DataHandler: ClassVar[type[MarketDataHandler]] = MarketDataHandler  # ty: ignore[invalid-mutable-override]
    StreamMessage: ClassVar[type[Any]] = MarketDataMessage

    @staticmethod
    def enc_hook(obj: Any) -> Any:  # ty: ignore[invalid-mutable-override]
        # e.g. a Bar's resolution, sent as its repr, e.g. "1m"
        if isinstance(obj, Resolution):
            return repr(obj)
        raise NotImplementedError(f"Encoding objects of type {type(obj)} is unsupported")

    @staticmethod
    def dec_hook(type_: type, obj: Any) -> Any:  # ty: ignore[invalid-mutable-override]
        if type_ is Resolution:
            return Resolution(obj)
        raise NotImplementedError(f"Decoding objects of type {type_} is unsupported")

    env: Environment | str
    product: BaseProduct
    resolution: Resolution | str

    def __str__(self) -> str:
        return ":".join(
            [
                self.env,
                super().__str__(),
                str(self.product.product_type),
                self.product.symbol,
                str(self.resolution),
            ]
        )

    @staticmethod
    def create_zmq_channel(product: BaseProduct, resolution: Resolution) -> ZeroMQChannel:
        """The zmq channel of a product's streamed market data, e.g. for a subscriber of the data engine's XPUB."""
        # NOTE: the trailing "." marks the end of the product name. A subscription matches any channel starting with it,
        # without it, subscribing to "BYBIT.1m.BTC" would also receive "BYBIT.1m.BTC_USDT"
        return f"{product.source}.{resolution!r}.{product.name}."

    def to_zmq_route(self) -> tuple[ZeroMQChannel, ZeroMQTopic]:
        resolution = Resolution(self.resolution)
        return self.create_zmq_channel(self.product, resolution), resolution.to_channel()

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v: str | Environment) -> Environment:
        if isinstance(v, str):
            return Environment(v.upper())
        return v

    @field_validator("resolution", mode="before")
    @classmethod
    def _validate_resolution(cls, v: str | Resolution) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @field_serializer("resolution")
    def _serialize_resolution(self, value: Resolution) -> str:
        return repr(value)
