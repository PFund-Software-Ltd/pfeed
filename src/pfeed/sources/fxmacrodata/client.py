from pfeed.client import DataClient
from pfeed.enums import DataCategory
from pfeed.sources.fxmacrodata.announcement_feed import FXMacroDataAnnouncementFeed
from pfeed.sources.fxmacrodata.mixin import FXMacroDataMixin


class FXMacroData(FXMacroDataMixin, DataClient):
    """PFeed client for the FXMacroData economic-announcement API."""

    announcement_feed: FXMacroDataAnnouncementFeed

    def _create_feeds(self):
        self.announcement_feed = FXMacroDataAnnouncementFeed(
            pipeline_mode=self._pipeline_mode,
            num_workers=(
                self._num_workers.get(DataCategory.ANNOUNCEMENT_DATA, None)
                if isinstance(self._num_workers, dict)
                else self._num_workers
            ),
        )
        self._feeds = [self.announcement_feed]
