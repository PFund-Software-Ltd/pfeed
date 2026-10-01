from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.sources.pfund.engine_data_model import PFundEngineDataModel

from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseDataMetadata


class PFundEngineDataMetadata(BaseDataMetadata):
    pass


class PFundEngineDataHandler(BaseDataHandler):
    _data_model: PFundEngineDataModel
    Metadata: ClassVar[type[PFundEngineDataMetadata]] = PFundEngineDataMetadata
