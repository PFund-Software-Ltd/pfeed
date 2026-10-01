from __future__ import annotations

from typing import ClassVar

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.sources.pfund.engine_data_handler import PFundEngineDataHandler


class PFundEngineDataModel(BaseDataModel):
    DataHandler: ClassVar[type[PFundEngineDataHandler]] = PFundEngineDataHandler
