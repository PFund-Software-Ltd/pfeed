from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    
from pfund.enums import Environment


# useful when using pfund's engine as an aggreagated data source of multiple trading venues with standardized data format
class StreamAPI:
    def __init__(
        self, 
        env: tEnvironment,
        # channels: PFundChannels
    ):
        self._env = Environment[env.upper()]
        # TODO: this should be a ws client as an external listener to mtflow's ws server
        self._ws_client = None