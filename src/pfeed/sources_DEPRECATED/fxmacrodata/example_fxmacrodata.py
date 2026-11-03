"""Download FXMacroData announcement history with PFeed.

Without FXMACRODATA_API_KEY set, only the most recent 90 days of USD data are returned.
"""

import pfeed as pe

feed = pe.FXMacroData().announcement_feed
announcements = feed.download(currency="USD", indicator="inflation")

print(announcements.collect())
print(feed.last_response_metadata.get("freemium_window"))
