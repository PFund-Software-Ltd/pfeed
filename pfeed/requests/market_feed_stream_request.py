from typing import Literal

from pfund.datas.data_config import DataConfig
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType
from pfeed.storages.storage_config import StorageConfig
from pfeed._io.io_config import IOConfig
from pfeed._sinks.sink_config import SinkConfig


class MarketFeedStreamRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.stream] = ExtractType.stream
    data_config: DataConfig | None = None
    sink_config: SinkConfig | None = None

    def is_streaming(self) -> bool:
        return True

    def finalize_load_config(self, storage_config: StorageConfig | None, io_config: IOConfig | None) -> None:
        super().finalize_load_config(storage_config, io_config)
        if storage_config and not self.clean_data:
            raise RuntimeError("Writing raw data in streaming is not supported")
