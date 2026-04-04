from __future__ import annotations
from typing import ClassVar

from pydantic import field_validator

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.sources.pfund.engine_data_handler import EngineDataHandler


class EngineDataModel(BaseDataModel):
    data_handler_class: ClassVar[type[EngineDataHandler]] = EngineDataHandler
    